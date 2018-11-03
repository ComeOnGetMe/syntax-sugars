import sys
import logging
import atexit
import multiprocessing as mp
from Queue import Empty
import traceback
from collections import defaultdict


def do_work(iptq, optq):
    while atexit._exithandlers:  # NOTE: this guarantee workers killed at exit
        atexit._exithandlers.pop()
    while True:
        pid, f, args = iptq.get(timeout=100000)
        try:
            ret = f(*args)
            optq.put((pid, ret))
        except:  # NOTE: this guarantee worker not shutdown by f raising exceptions
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

    @property
    def alive_procs(self):
        return [pid for pid, proc in self.procs.iteritems() if proc.is_alive()]

    def _terminate_proc(self):
        for _, proc in self.procs.iteritems():
            proc.terminate()
        self.logger.info('Exited')

    def _get_process_id(self, traj_id):
        return traj_id % self.proc_num

    def _restart_fail_process(self, fail_pid):
        self.logger.info("Restarting {}".format(' '.join(map(str, fail_pid))))
        self.optq.close()
        self.optq = mp.Queue()
        for process_id in fail_pid:
            self.iptq[process_id].close()
            self.procs[process_id].terminate()

            self.iptq[process_id] = mp.Queue()
            self.procs[process_id] = mp.Process(target=do_work, args=(self.iptq[process_id], self.optq))
            self.procs[process_id].start()

    def _split_ipt(self, ipt_dict, meta):
        proc_ipt = defaultdict(list)
        for ipt_key, ipt in ipt_dict.iteritems():
            ipt_pid = self._get_process_id(ipt_key)
            proc_ipt[ipt_pid].append(ipt)
        if meta is not None:  # NOTE: every process should be entered if meta is not None
            for pid in self.proc_id_pool:
                proc_ipt[pid] = meta, proc_ipt.get(pid, [])
        return proc_ipt

    def distribute(self, proc_ipt, func, agg_func):
        put_rec = set([])
        for pid, process in self.procs.iteritems():
            agg_proc_ipt = agg_func(proc_ipt[pid])
            self.iptq[pid].put((pid, func, agg_proc_ipt))
            put_rec.add(pid)
            self.logger.info("Put {} objs into process {}".format(len(agg_proc_ipt), pid))
        return put_rec

    def collect(self, agg_func):  # TODO: better proc life check?
        suc_proc = set()
        opt_dict = {}
        for _ in range(self.proc_num):
            try:
                pid, proc_opt = self.optq.get(timeout=self.timeout)
                suc_proc.add(pid)
                opt_dict[pid] = proc_opt
            except Empty:
                pass
        agg_opt_dict = agg_func(opt_dict)
        return agg_opt_dict, suc_proc

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

    def run(self, ipt_dict, func, metadata=None, agg_ipt=None, agg_opt=None):
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
        if agg_ipt is None:
            agg_ipt = self._aggregate_ipt
        if agg_opt is None:
            agg_opt = self._aggregate_opt
        assert callable(agg_ipt) and callable(agg_opt)

        proc_ipt_dict = self._split_ipt(ipt_dict, metadata)
        put_proc = self.distribute(proc_ipt_dict, func, agg_ipt)
        opt, suc_proc = self.collect(agg_opt)
        failed_proc = put_proc - suc_proc
        if len(failed_proc) > 0:
            self.logger.warning("Proc {} failed.".format(' '.join(map(str, failed_proc))))
        return opt


if __name__ == '__main__':
    def add_one(*lst):
        return [x + 1 for x in lst]

    def add_something(someth, lst):
        return [x + someth for x in lst]

    def fail_sometime(*args):
        if any(x == 3 for x in args):
            raise ValueError
        return args

    logging.basicConfig(level=logging.INFO)
    mmp = MultiProcessor(proc_num=4, timeout=0.1)
    ipt_lst = range(7)
    hash_dict = {idx: ipt for idx, ipt in enumerate(ipt_lst)}

    """ Basic usage """
    opt = mmp.run(hash_dict, add_one)
    print opt

    """ with meta variable """
    opt = mmp.run(hash_dict, add_something, metadata=100)
    print opt

    """ Fail proc """
    opt = mmp.run(hash_dict, fail_sometime)
    print opt
    opt = mmp.run(hash_dict, add_one)
    print opt
