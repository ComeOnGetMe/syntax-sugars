import sys
import logging
import atexit
import multiprocessing as mp
from Queue import Empty
import traceback
from collections import defaultdict


def do_work(iptq, optq):
    while True:
        pid, f, args = iptq.get()
        try:
            ret = f(*args)
            optq.put((pid, ret))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)


class MultiProcessor(object):
    def __init__(self, proc_num=1, timeout=.1, single_proc=False):
        self.logger = logging.getLogger(type(self).__name__)
        self.proc_num = proc_num
        self.timeout = timeout
        self.single_mode = single_proc  # TODO
        self.proc_id_pool = set(range(self.proc_num))
        self.iptq = {process_id: mp.Queue() for process_id in self.proc_id_pool}
        self.optq = mp.Queue()
        self.procs = {process_id: mp.Process(target=do_work, args=(self.iptq[process_id], self.optq))
                      for process_id in self.proc_id_pool}

        atexit.register(self._terminate_proc)
        for i, p in self.procs.iteritems():
            p.start()

    def _terminate_proc(self):
        for _, proc in self.procs.iteritems():
            proc.terminate()
        self.logger.info('Successfully exit fusion')

    def _get_process_id(self, traj_id):
        return traj_id % self.proc_num

    def _restart_fail_process(self, fail_pid):
        self.logger.info("Restarting {}".format(' '.join(map(str, fail_pid))))
        self.optq.close()
        self.optq = mp.Queue()
        for process_id in fail_pid:  # TODO: test this
            self.iptq[process_id].close()
            self.procs[process_id].terminate()

            self.iptq[process_id] = mp.Queue()
            self.procs[process_id] = mp.Process(target=do_work, args=(self.iptq[process_id], self.optq))
            self.procs[process_id].start()

    def _split_ipt(self, ipt_dict):
        proc_ipt = defaultdict(list)
        for ipt_key, ipt in ipt_dict.iteritems():
            ipt_pid = self._get_process_id(ipt_key)
            proc_ipt[ipt_pid].append(ipt)
        return proc_ipt

    def distribute(self, proc_ipt, func):
        put_rec = set([])
        for pid, process in self.procs.iteritems():
            agg_proc_ipt = self._aggregate_ipt(proc_ipt[pid])
            self.iptq[pid].put((pid, func, agg_proc_ipt))
            put_rec.add(pid)
            self.logger.info("Put {} objs into process {}".format(len(agg_proc_ipt), pid))
        return put_rec

    def collect(self):  # TODO: better proc life check?
        suc_proc = set()
        opt_dict = {}
        for _ in range(self.proc_num):
            try:
                pid, proc_opt = self.optq.get(timeout=self.timeout)
                suc_proc.add(pid)
                opt_dict[pid] = proc_opt
            except Empty:
                pass
        return opt_dict, suc_proc

    def _aggregate_ipt(self, ipt_lst):
        """
        This function is used to combine split inputs for each process.
        Default is the list given by _split_ipt
        :param ipt_lst:
        :return:
        """
        return ipt_lst

    def _aggregate_opt(self, proc_opt_dict):
        """
        This function is used to combine outputs from all processes.
        Default action is put them into a list.
        :param proc_opt_dict:
        :return:
        """
        opt_lst = []
        for pid, opt in proc_opt_dict.iteritems():
            opt_lst.append(opt)
        return opt_lst

    def run(self, ipt_dict, func):
        """
        Main function of multiprocessing.
        :param ipt_dict: Pair input with a key to indicate worker id.
            Multiprocessor will use the key and _get_process_id method to
            allocate inputs to different workers. All allocated inputs will be
            combined and passed as a list.
        :param func: function to run those inputs.
        :return: outputs from workers are collected and aggregated using _aggregate. If not inherited, it will
            return in the format of {pid: output}.
        """
        proc_ipt_dict = self._split_ipt(ipt_dict)
        put_proc = self.distribute(proc_ipt_dict, func)
        proc_opt_dict, suc_proc = self.collect()
        failed_proc = put_proc - suc_proc
        if len(failed_proc) > 0:
            self.logger.warning("Proc {} failed.".format(' '.join(map(str, failed_proc))))
            self._restart_fail_process(failed_proc)
        ret = self._aggregate_opt(proc_opt_dict)
        return ret


if __name__ == '__main__':
    def addone(*lst):
        return [x + 1 for x in lst]

    mmp = MultiProcessor(proc_num=4, timeout=100000)
    ipt_lst = range(100, 107)
    ipt_dict = {idx: ipt for idx, ipt in enumerate(ipt_lst)}
    opt = mmp.run(ipt_dict, addone)
    print opt
