#!/usr/bin/env python


# LEBL: created based on deps.py to get task inputs
#


from __future__ import print_function
import luigi.interface
from luigi.contrib.ssh import RemoteTarget
from luigi.postgres import PostgresTarget
from luigi.s3 import S3Target
from luigi.target import FileSystemTarget
from luigi.task import flatten
from luigi import parameter
import sys
from luigi.cmdline_parser import CmdlineParser
import collections


def get_task_requires(task):
    return set(flatten(task.requires()))


def dfs_paths(start_task, goal_task_family, path=None):
    if path is None:
        path = [start_task]
    if start_task.task_family == goal_task_family or goal_task_family is None:
        for item in path:
            yield item
    for next in get_task_requires(start_task) - set(path):
        for t in dfs_paths(next, goal_task_family, path + [next]):
            yield t


class upstream(luigi.task.Config):
    '''
    Used to provide the parameter upstream-family
    '''
    family = parameter.Parameter(default=None)


def find_deps(task, upstream_task_family):
    '''
    Finds all dependencies that start with the given task and have a path
    to upstream_task_family

    Returns all deps on all paths between task and upstream
    '''
    return set([t for t in dfs_paths(task, upstream_task_family)])


def find_deps_cli():
    '''
    Finds all tasks on all paths from provided CLI task
    '''
    cmdline_args = sys.argv[1:]
    with CmdlineParser.global_instance(cmdline_args) as cp:
        return find_deps(cp.get_task_obj(), upstream().family)


def get_task_input_description(task_input, level=0):
    '''
    Returns a task's input as a string
    '''
    input_description = "n/a"

    if isinstance(task_input, collections.Iterable):
        input_description = '\n'.join([str('  ' * level) + get_task_input_description(_input, level+1) for _input in task_input])
    elif isinstance(task_input, RemoteTarget):
        input_description = "[SSH] {0}:{1}".format(task_input._fs.remote_context.host, task_input.path)
    elif isinstance(task_input, S3Target):
        input_description = "[S3] {0}".format(task_input.path)
    elif isinstance(task_input, FileSystemTarget):
        input_description = "[FileSystem] {0}".format(task_input.path)
    elif isinstance(task_input, PostgresTarget):
        input_description = "[DB] {0}:{1}".format(task_input.host, task_input.table)
    else:
        input_description = "to be determined"

    return input_description


def main():
    deps = find_deps_cli()
    for task in deps:
        task_input = task.input()

        if isinstance(task_input, dict):
            input_descriptions = [get_task_input_description(input) for label, input in task_input.items()]
        elif isinstance(task_input, collections.Iterable):
            input_descriptions = [get_task_input_description(input) for input in task_input]
        else:
            input_descriptions = [get_task_input_description(task_input)]

        print("   TASK: {0}".format(task))
        for desc in input_descriptions:
            tokens = desc.split('\n')
            for token in tokens:
                print("                       : {0}".format(token))


if __name__ == '__main__':
    main()
