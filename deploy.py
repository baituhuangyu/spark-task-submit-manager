#!/usr/bin/env python
# encoding: utf-8

from common.utils.fabric_utils import Deploy
import argparse
from conf.conf import WORK_DIR
import os
import getpass


def build_run_task_fs(model="all", cls_name="", task_name="run.py"):
    task_fs_str = """
from controller.main_controller import Controller
from functools import partial


def run_model(model="all", cls_name=None):
    if model == "all":
        run = Controller().run_all
    elif model == "single" and cls_name and isinstance(cls_name, str):
        run = partial(Controller().run_single, cls_name=cls_name)
    else:
        raise Exception()

    return run


run_model(model="%s", cls_name="%s")()
""" % (model, cls_name)

    with open(os.path.join(WORK_DIR, task_name), "w") as fp:
        fp.write(task_fs_str)


def build_develop_user():
    develop_user_str = """#!/usr/bin/env python2
# encoding: utf-8


DEVELOP_USER = '%s'
""" % getpass.getuser()
    with open(os.path.join(WORK_DIR, "conf", "sub", "deploy_user.py"), "w") as fp:
        fp.write(develop_user_str)


def check_params(args):
    if args.m == "single" and args.cls == "":
        raise Exception("single 模式下，cls 不能为空， 运行 python deploy.py 获取帮助")

    if args.cls != "" and args.m == "all":
        raise Exception("参数错误， 运行 python deploy.py 获取帮助")

    if args.core > 40 or args.core < 1:
        raise Exception("core 可选范围为 1-40， 运行 python deploy.py 获取帮助")


if __name__ == '__main__':
    task_name = "run.py"

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-a', help='action. set_env, deploy_submit. （指定运行模式）', default='deploy_submit', type=str, choices=["set_env", "deploy_submit"])
    parser.add_argument('-m', help='model. all, single. （提交运行的方式， all运行所有， single， 运行单一 class，运行单一class时需要指定cls）', default='single', choices=["all", "single"])
    parser.add_argument('-cls', help='class name to run in single model。 单一模式下需要指定的cls', type=str, default="")
    parser.add_argument('-memory', help='指定spark submit时运行的最大内存, 可选范围为1G-120G', type=str, default="20G")
    parser.add_argument('-core', help='指定spark submit时运行的内存， 可选范围为 1-40', type=int, default=8)
    parser.add_argument('--help', action='help')
    args = parser.parse_args()
    build_run_task_fs(args.m, args.cls, task_name)
    build_develop_user()

    if args.a == "deploy_submit":
        Deploy().deploy()
        Deploy().run_submit_task(task_name, executor_memory=args.memory, total_executor_cores=args.core)
    elif args.a == "set_env":
        Deploy().deploy()
        Deploy().setup_py_env()
    else:
        raise Exception("please run `python deploy.py` --help to get help")
