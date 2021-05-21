#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Created by tom on 10/10/16
#  Changed on 2019/7/31 version: 2.0.0

import random
import os
from os.path import getsize, exists
import time
# fcntl is only for linux
import fcntl
import shutil
from shutil import copyfile
import logging
from logging.handlers import RotatingFileHandler
import socket
import re
import traceback
from datetime import datetime, time, date
import ConfigParser
from optparse import OptionParser
from filecmp import dircmp
from subprocess import Popen, PIPE
import pprint
import git
from git import GitCommandError
from git.repo import Repo
from gitdb.exc import BadName
import sys
import errno
from netaddr.ip import IPAddress

g_script_path = sys.path[1]
g_remote_url_list = []
g_git_local_dir = ''
g_git_merge_dir = ''
g_adns_conf_dir = ''
g_delete_white_list = []
g_git_log = ''
g_git_clean_time = ''

LOG_ROTATE_MAXBytes = 100 * 1024 * 1024
MAX_LOG_FILES_COUNT = 5


class LogRotator(object):
    def __init__(self, prefix, suffix):
        self.prefix = prefix
        self.suffix = suffix

    def __str__(self):
        return "{}[x].{}".format(self.suffix, self.prefix)

    def __touch(self, file_name):
        open(file_name, 'w').close()

    def rotate(self):
        files = ["{}{}.{}".format(self.prefix, x + 1, self.suffix)
                 for x in range(MAX_LOG_FILES_COUNT)]

        [self.__touch(f) for f in files if not exists(f)]

        current_log = "{}.{}".format(self.prefix, self.suffix)
        if not exists(current_log):
            self.__touch(current_log)

        if (getsize(current_log) < LOG_ROTATE_MAXBytes):
            return

        files.insert(0, current_log)

        for i in range(len(files) - 1, 0, -1):
            copyfile(files[i-1], files[i])

        self.__touch(files[0])


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def rm_dir(path):
    command = ['/usr/bin/rm', '-rf', path]
    logging.info(command)
    ret = run_command(command)
    if ret == None:
        logging.info('========run stop========\n\n')
        sys.exit(1)
        
def time_in_range(start, end, check_time=None):
    check_time = check_time or datetime.now().time()
    if start <= end:
        return start <= check_time <= end
    else:# crosses midnight
        return start <= check_time or check_time <= end

def is_already_cleaned(git_local, clean_now):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    remote_name = git_local.split('/')[-1]
    clean_time_log = g_git_clean_time + '.' + remote_name
    if not os.path.exists(clean_time_log):
        #创建并打开
        logging.info('create and open [%s].', clean_time_log)
        with open(clean_time_log, "w+") as f:
            f.write(today_str)
            return False
    else:
        #打开
        with open(clean_time_log, "r+") as f:
            line = f.readline()
            if line == today_str and not clean_now:
                return True
            else:
                f.seek(0)
                f.truncate()
                f.write(today_str)
                return False

def clean_local_git(clean_now, git_local, remote_url):
    #判断目录是否存在
    if not os.path.exists(git_local):
        logging.info('git_local[%s] not exist, no need to clean.', git_local)
        return

    #判断是否为清理时间(凌晨4点-24点)
    if not time_in_range(time(4, 0), time(23, 0)) and not clean_now:
        logging.info('git_local[%s] now[%s], not in clean time range[4:00-23:00].', git_local, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return

    #判断今天是否已经清理过
    if is_already_cleaned(git_local, clean_now):
        logging.info('git_local[%s] is already cleaned, just continue.', git_local)
        return

    #清理
    git_local_new = git_local + '.new'
    if os.path.exists(git_local_new):
        logging.info('rm dir[%s].', git_local_new)
        rm_dir(git_local_new)
    logging.info('git clone from [%s] to [%s], and depth=1.', remote_url, git_local_new)
    repo_clone = Repo.clone_from(remote_url, git_local_new, depth=1)
    if not repo_clone:
        logging.error(
            "clone repo from {0} to {1}failed.".format(remote_url, git_local_new))
        logging.info('========run stop========\n\n')
        sys.exit(1)
    git_local_bak = git_local + '.bak'
    if os.path.exists(git_local_bak):
        logging.info('rm dir[%s].', git_local_bak)
        rm_dir(git_local_bak)
    logging.info('mv dir[%s] to dir[%s].', git_local, git_local_bak)
    shutil.move(git_local, git_local_bak)
    logging.info('mv dir[%s] to dir[%s].', git_local_new, git_local)
    shutil.move(git_local_new, git_local)

class FileLock(object):
    def __init__(self, file_name):
        if not os.path.exists("/var/run/lock/{0}".format(file_name)):
            if not os.path.exists("/var/run/lock"):
                os.makedirs("/var/run/lock")
            self.file_obj = open("/var/run/lock/{0}".format(file_name), 'a')
        else:
            self.file_obj = open("/var/run/lock/{0}".format(file_name))

    def __enter__(self):
        try:
            fcntl.flock(self.file_obj, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as e:
            logging.error(
                "Lock File ERROR, got exception={0}".format(str(e)))
            self.file_obj.close()
            logging.info('========run stop========\n\n')
            sys.exit(1)
        return self.file_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        fcntl.flock(self.file_obj, fcntl.LOCK_UN)
        self.file_obj.close()


def git_pull(remote_url, git_local):
    """
    @remote_url: git remote address
    @git_local: local git repo dir
    """
    logging.info('git start pull from remote repo.')
    # check if we need to clean the local git
    clean_local_git(False, git_local, remote_url)
    # git clone if not exist
    if not os.path.exists("{0}/.git".format(git_local)) or \
            not git.repo.fun.is_git_dir("{0}/.git".format(git_local)):
        logging.info('git clone from [%s] to [%s], and depth=1.', remote_url, git_local)
        repo_clone = Repo.clone_from(remote_url, git_local,depth=1)
        if not repo_clone:
            logging.error(
                "clone repo from {0} to {1}failed.".format(remote_url, git_local))
            logging.info('========run stop========\n\n')
            sys.exit(1)

    # get repo
    repo = Repo(git_local)
    type(repo.git).GIT_PYTHON_TRACE = 'full'
    remote = None

    # get remote url
    for item in repo.remotes:
        if item.url == remote_url:
            remote = item
            break
    if remote is None:
        clean_local_git(True, git_local, remote_url)

    # start pull
    ssh_executable = os.path.join(g_script_path, 'debug_ssh.sh')
    with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_executable):
        try:
            logging.info('git pull from remote=%s, url=%s',
                         remote.name, remote.url)
            info_list = remote.pull()
            for info in info_list:
                ret = info.flags & (git.FetchInfo.REJECTED |
                                    git.FetchInfo.ERROR)
            if ret > 0:
                logging.warning("git pull from remote ERROR: %s", info_list)
                logging.info('========run stop========\n\n')
                sys.exit(1)
            else:
                logging.info("git pull return mesg: %s", info_list)
        except BadName:
            logging.warn("git pull warn: ", exc_info=True)
        except Exception:
            clean_local_git(True, git_local, remote_url)

    # get last commit msg
    last_commit_id = str(repo.head.commit)

    return last_commit_id


def walk_dir(dir_in, files_out):
    for root, _, files in os.walk(dir_in, followlinks=True):
        for f in files:
            file_out = os.path.join(root, f)
            files_out.append(file_out)


def get_diff_files(dcmp, diff_items, del_items):
    for name in dcmp.diff_files:
        diff_fn = os.path.join(dcmp.left, name)
        diff_items.append(diff_fn)

    left_only = dcmp.left_only
    if left_only:
        for name in left_only:
            left_fn = os.path.join(dcmp.left, name)
            if os.path.isdir(left_fn):
                walk_dir(left_fn, diff_items)
            diff_items.append(left_fn)

    right_only = dcmp.right_only
    if right_only:
        for name in dcmp.right_only:
            right_fn = os.path.join(dcmp.right, name)
            if os.path.isdir(right_fn):
                walk_dir(right_fn, del_items)
            del_items.append(right_fn)

    for sub_dcmp in dcmp.subdirs.values():
        get_diff_files(sub_dcmp, diff_items, del_items)


def copy_diff(src_dir, dest_dir, diff_items):
    update_fns = []
    for source in diff_items:
        dest = source.replace(src_dir, dest_dir)
        if os.path.isdir(source):
            mkdir_p(dest)
            logging.info('make dir: %s' % dest)
        else:
            if not os.path.exists(os.path.dirname(dest)):
                mkdir_p(os.path.dirname(dest))
            shutil.copyfile(source, dest)
            update_fns.append(dest)
            logging.info('copy file: %s' % dest)

    return update_fns


def remove_del_files(src_dir, dst_dir, del_items):
    delete_fns = []
    for name in del_items:
        file_path = name.replace(src_dir, dst_dir)
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
            logging.info('remove dir: %s' % file_path)
        else:
            if os.path.exists(file_path):
                os.remove(file_path)
            delete_fns.append(file_path)
            logging.info('remove file:%s' % file_path)

    return delete_fns


def run_command(command):
    p = Popen(command, stdout=PIPE, stderr=PIPE, close_fds=True)
    p_std = p.stdout.read()
    p_stderr = p.stderr.read()
    if p_std:
        logging.info(p_std.rstrip())
    if p_stderr:
        logging.error(p_stderr.rstrip())
        return None
    return p_std


def is_valid_ip(ipstring):
    try:
        IPAddress(ipstring)
        return True
    except:
        return False


def set_environment(key, value):
    os.environ[key] = value


def get_environment(key):
    if key in os.environ:
        return os.environ[key]
    else:
        return None


def get_hostname_from_url(url):
    ret = re.findall(':', url)
    if len(ret) > 2:
        logging.warn('find ipv6 in url[%s], not support.', url)
        return None
    # url-like: http://git@git.jd.com:dns-anti/tp-advanced-ddos.git
    if (url.find('//') != -1 and url.find('@') != -1):
        hostname = re.split('[@:/]', url)[4]
    # url-like: http://git.jd.com/dns-anti/tp-advanced-ddos.git
    elif (url.find('//') != -1):
        hostname = re.split('[@:/]', url)[3]
    # url-like: git@git.jd.com:dns-anti/tp-advanced-ddos.git
    elif (url.find('@') != -1):
        hostname = re.split('[@:/]', url)[1]
    # url-like: git.jd.com:dns-anti/tp-advanced-ddos.git
    else:
        hostname = re.split('[@:/]', url)[0]

    return hostname


def update_hosts(hostname, ip):
    with open('/etc/hosts', "r") as f:
        lines = (line.rstrip() for line in f)
        # 删除匹配行
        altered_lines = [line for line in lines if line.find(
            hostname) == -1]
        # 增加新行
        altered_lines.append(ip + ' ' + hostname)
    with open('/etc/hosts', "w") as f:
        f.write('\n'.join(altered_lines) + '\n')


def refresh_ip_for_hostname(hostname, timeout_second):
    # 如果传入是IP地址，则直接返回；
    if is_valid_ip(hostname):
        return True

    # 如果dig返回成功，则更新/etc/hosts文件并返回；
    command = ['dig', '+short',
               '+time='+str(timeout_second), hostname]
    logging.info(command)
    ret = run_command(command)
    if ret != None:
        ret_list = ret.split('\n')
        for item in ret_list:
            if is_valid_ip(item):
                update_hosts(hostname, item)
                return True
        logging.error(
            'cannot get ip for host[%s], command ret[%s]', hostname, ret)
        return False
    else:
        logging.error(
            'cannot get ip for host[%s], [%s] command ERROR.', hostname, command)
        return False


def reload_user_view(dns_admin, update_files):
    for name in update_files:
        if (name.find('/views/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            command = [dns_admin, '--reload-user-view', file_name]
            logging.info(command)
            run_command(command)
    update_files = filter(lambda x: x.find('/views/') != -1, update_files)


def delete_user_view(dns_admin, del_files):
    for name in del_files:
        if (name.find('/views/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            command = [dns_admin, '--delete-user-view', file_name]
            logging.info(command)
            run_command(command)
    del_files = filter(lambda x: x.find('/views/') != -1, del_files)


def reload_zones(dns_admin, update_files):
    for name in update_files:
        if (name.find('/zones/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            command = [dns_admin, '--reload-zone', file_name]
            logging.info(command)
            run_command(command)
    update_files = filter(lambda x: x.find('/zones/') != -1, update_files)


def delete_zones(dns_admin, del_files):
    for name in del_files:
        if (name.find('/zones/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            command = [dns_admin, '--delete-zone', file_name]
            logging.info(command)
            run_command(command)
    del_files = filter(lambda x: x.find('/zones/') != -1, del_files)

def reload_policys(dns_admin, update_files):
    for name in update_files:
        if (name.find('/policys/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            if(name.find('srcip_whitelist') != -1):
                command = [dns_admin, '--reload-srcip-whitelist']
            elif (name.find('srcip_blacklist') != -1):
                command = [dns_admin, '--reload-srcip-blacklist']
            elif (name.find('domain_whitelist') != -1):
                command = [dns_admin, '--reload-domain-whitelist']
            else:
                logging.error('policy[%s] not support.', name)
                continue
            logging.info(command)
            run_command(command)
    update_files = filter(lambda x: x.find('/policys/') != -1, update_files)


def delete_policys(dns_admin, del_files):
    for name in del_files:
        if (name.find('/policys/') != -1):
            # get file name
            file_name = os.path.split(name)[1]
            if(name.find('srcip_whitelist') != -1):
                command = [dns_admin, '--reload-srcip-whitelist']
            elif (name.find('srcip_blacklist') != -1):
                command = [dns_admin, '--reload-srcip-blacklist']
            elif (name.find('domain_whitelist') != -1):
                command = [dns_admin, '--reload-domain-whitelist']
            else:
                logging.error('policy[%s] not support.', name)
                continue
            logging.info(command)
            run_command(command)
    del_files = filter(lambda x: x.find('/policys/') != -1, del_files)


def init_log():
    # log_path = '/export/tp-dns/log/'
    global g_git_log
    log_path = g_script_path + '/log/'
    mkdir_p(log_path)
    # git.log
    git_log_prefix = log_path + '/git'
    git_log_suffix = 'log'
    g_git_log = git_log_prefix + '.' + git_log_suffix
    logRotator = LogRotator(git_log_prefix, git_log_suffix)
    logRotator.rotate()
    # ssh.log
    ssh_log_prefix = log_path + '/ssh'
    ssh_log_suffix = 'log'
    logRotator = LogRotator(ssh_log_prefix, ssh_log_suffix)
    logRotator.rotate()
    # conf-pull.log
    conf_pull_log = log_path + '/conf-pull.log'
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = RotatingFileHandler(
        conf_pull_log, maxBytes=100*1024*1024, backupCount=10)
    datefmt = '%Y-%m-%d %H:%M:%S'
    format_str = '%(asctime)s %(levelname)s %(message)s'
    formatter = logging.Formatter(format_str, datefmt)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # git clean time log
    global g_git_clean_time
    g_git_clean_time = log_path + '/conf-pull-clean-time'

def log_commit_id(commit_id_list):
    last_commit_log = g_script_path + '/log/' + '/last_commit.log'
    with open(last_commit_log, 'w') as f:
        for item in commit_id_list:
            f.write(item)


def get_now_time():
    nowTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return nowTime


def dir_merge(current_git_local, remote_name):
    git_merge_zones_dir = g_git_merge_dir + "/zones/"
    git_merge_views_dir = g_git_merge_dir + "/views/"
    if not os.path.exists(git_merge_zones_dir):
        mkdir_p(git_merge_zones_dir)

    if not os.path.exists(git_merge_views_dir):
        mkdir_p(git_merge_views_dir)

    if not os.path.exists(g_git_merge_dir + '/ip_range.map') and os.path.exists(current_git_local + '/ip_range.map'):
        os.symlink(current_git_local + '/ip_range.map',
                   g_git_merge_dir + '/ip_range.map')

    if not os.path.exists(g_git_merge_dir + '/ipv6_range.map') and os.path.exists(current_git_local + '/ipv6_range.map'):
        os.symlink(current_git_local + '/ipv6_range.map',
                   g_git_merge_dir + '/ipv6_range.map')

    if not os.path.exists(g_git_merge_dir + '/view_name_id.map') and os.path.exists(current_git_local + '/view_name_id.map'):
        os.symlink(current_git_local + '/view_name_id.map',
                   g_git_merge_dir + '/view_name_id.map')

    # 如果zones目录为新增，则新建软连接；
    if not os.path.exists(git_merge_zones_dir + remote_name) and os.path.exists(current_git_local + '/zones/'):
        os.symlink(current_git_local + '/zones/',
                   git_merge_zones_dir + remote_name)
    # 如果zones目录被清空，需要删除软连接文件；
    if not os.path.exists(current_git_local + '/zones/') and os.path.islink(git_merge_zones_dir + remote_name):
        os.unlink(git_merge_zones_dir + remote_name)

    # 如果views目录为新增，则新建软连接；
    if not os.path.exists(git_merge_views_dir + remote_name) and os.path.exists(current_git_local + '/views/'):
        os.symlink(current_git_local + '/views/',
                   git_merge_views_dir + remote_name)
    # 如果views目录被清空，需要删除软连接文件；
    if not os.path.exists(current_git_local + '/views/') and os.path.islink(git_merge_views_dir + remote_name):
        os.unlink(git_merge_views_dir + remote_name)

    # 如果policys目录为新增，则新建软连接；
    if not os.path.exists(g_git_merge_dir + '/policys') and os.path.exists(current_git_local + '/policys'):
        os.symlink(current_git_local + '/policys',
                   g_git_merge_dir + '/policys')
    # 如果policys目录被清空，需要删除软连接文件；
    if not os.path.exists(current_git_local + '/policys') and os.path.islink(g_git_merge_dir + '/policys'):
        os.unlink(g_git_merge_dir + '/policys')


def delete_error_commit_id(commit_id_list, error_file):
    remote_name = error_file.split('/')[5]
    delete_index = []
    for index, commit_str in enumerate(commit_id_list):
        if commit_str.split()[2].split('/')[-1] == remote_name:
            delete_index.append(index)
    # start at the end to avoid recomputing offsets
    for index in reversed(delete_index):
        del commit_id_list[index]


def check_items(diff_items, del_items, commit_id_list):
    adns_check_bin = g_script_path + '/../bin/adns_check'
    # check diff files
    delete_index = []
    for index, item in enumerate(diff_items):
        if os.path.isfile(item):
            command = [adns_check_bin, item]
            logging.info(command)
            rcode = run_command(command)
            if rcode == None:
                delete_index.append(index)
                delete_error_commit_id(commit_id_list, item)
    # start at the end to avoid recomputing offsets
    for index in reversed(delete_index):
        del diff_items[index]

    # check delete files
    if len(g_delete_white_list):
        delete_index = []
        for index, item in enumerate(del_items):
            if os.path.isfile(item):
                if (item.find('/views/') != -1):
                    continue
                else:
                    file_name = os.path.split(item)[1]
                    if any(file_name == s for s in g_delete_white_list):
                        logging.error(
                            'file: [%s] in g_delete_white_list, cannot be deleted.', item)
                        delete_index.append(index)
        # start at the end to avoid recomputing offsets
        for index in reversed(delete_index):
            del del_items[index]


def update_items(diff_items, del_items):
    dns_admin = g_script_path + '/../bin/adns_adm'
    # copy files from git merge to adns
    update_files = copy_diff(g_git_merge_dir, g_adns_conf_dir, diff_items)
    logging.info('update files:%s' % pprint.pformat(update_files))

    # delete files from adns
    delete_files = remove_del_files(
        g_git_merge_dir, g_adns_conf_dir, del_items)
    logging.info('delete files:%s' % pprint.pformat(delete_files))

    # user view update
    if update_files:
        reload_user_view(dns_admin, update_files)

    # zone update
    if update_files:
        reload_zones(dns_admin, update_files)

    # policy update
    if update_files:
        reload_policys(dns_admin, update_files)

    # user view delete
    if delete_files:
        delete_user_view(dns_admin, delete_files)

    # zone delete
    if delete_files:
        delete_zones(dns_admin, delete_files)

    # policy delete
    if delete_files:
        delete_policys(dns_admin, delete_files)


def read_config():
    global g_remote_url_list
    global g_git_local_dir
    global g_git_merge_dir
    global g_adns_conf_dir
    global g_delete_white_list
    conf_path = g_script_path + '/conf-pull.conf'
    try:
        # for general enviorment
        conf = ConfigParser.ConfigParser()
        conf.read(conf_path)

        remote_urls_str = conf.get("git", "remote_urls")
        g_git_local_dir = conf.get("git", "git_local_dir")
        g_git_merge_dir = conf.get("git", "git_merge_dir")
        g_adns_conf_dir = conf.get("git", "adns_conf_dir")
        white_list_str = conf.get("delete", "white_list")

        g_remote_url_list = remote_urls_str.replace(
            '\n', '').replace('\r', '').split(',')
        g_delete_white_list = white_list_str.replace(
            '\n', '').replace('\r', '').split(',')
    except Exception:
        logging.error("read conf-pull.conf ERROR: ", exc_info=True)
        logging.info('========run stop========\n\n')
        sys.exit(1)

    if not os.path.exists(g_git_local_dir):
        mkdir_p(g_git_local_dir)

    if not os.path.exists(g_adns_conf_dir):
        mkdir_p(g_adns_conf_dir)


def init_enviorment():
    set_environment('GIT_TRACE', g_git_log)
    set_environment('ADNS_BIN_PATH', g_script_path)
    command = ['git', 'config', '--global', 'gc.auto', '1']
    logging.info(command)
    run_command(command)


def refresh_host(url):
    hostname = get_hostname_from_url(url)
    if hostname != None:
        refresh_ip_for_hostname(hostname, 1)


def main():
    # init log
    init_log()
    logging.info('........run start........')
    try:
        # init enviorment
        init_enviorment()

        # read config
        read_config()

        with FileLock('conf-pull') as _flock:
            # iter remote urls
            commit_id_list = []
            for remote_url in g_remote_url_list:
                remote_name = remote_url.split('/')[-1]
                refresh_host(remote_url)
                current_git_local = g_git_local_dir + remote_name
                if not os.path.exists(current_git_local):
                    mkdir_p(current_git_local)
                # pull
                commit_id = git_pull(remote_url, current_git_local)
                last_commit_str = "\n" + get_now_time() + "\t" + remote_url + "\t" + commit_id
                commit_id_list.append(last_commit_str)
                # merge
                dir_merge(current_git_local, remote_name)

            # get change files
            diff_items = []
            del_items = []
            ignore_files = ['RCS', 'CVS', 'tags',
                            '.git', 'adns.conf', 'zones_list']
            dcmp = dircmp(g_git_merge_dir, g_adns_conf_dir, ignore_files)
            get_diff_files(dcmp, diff_items, del_items)
            if not diff_items and not del_items:
                logging.info('no diff files')
                logging.info('========run stop========\n\n')
                sys.exit(0)
            logging.info('diff items:%s' % pprint.pformat(diff_items))
            logging.info('del items:%s' % pprint.pformat(del_items))

            # check files
            check_items(diff_items, del_items, commit_id_list)

            # update files
            update_items(diff_items, del_items)

            # log the last success commit id for revert
            log_commit_id(commit_id_list)

            logging.info('========run stop========\n\n')
            sys.exit(0)
    except Exception:
        logging.error("conf pull ERROR: ", exc_info=True)
        logging.info('========run stop========\n\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
