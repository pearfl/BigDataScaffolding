import logging
import logging.handlers
import os
import gzip
import shutil
import urllib3
import subprocess
import json
from datetime import datetime, timedelta
import time
import argparse

import requests
import sys
from concurrent.futures import ThreadPoolExecutor

# 解析命令行参数
parser = argparse.ArgumentParser(description='应用筛选和终止脚本')
parser.add_argument('--hours', type=int, default=24, help='筛选应用的小时数，默认为24小时')
parser.add_argument('--log-dir', type=str, default='logs', help='日志目录，默认为"logs"')
parser.add_argument('--ip', type=str, default='', help='服务器IP地址，默认为127.0.0.1')
parser.add_argument('--port', type=int, default=, help='服务器端口，默认为')
parser.add_argument('--cert', type=str, default='', help='SSL证书路径（可选）')
args = parser.parse_args()

# 动态生成日志文件名
today = datetime.now()
log_file = f"yarn-resource-scheduler_{today.strftime('%Y%m')}.log"
log_dir = args.log_dir
os.makedirs(log_dir, exist_ok=True)

# 配置日志记录器
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 启用日志轮转和压缩
def compress_log(source, target):
    with open(source, 'rb') as src_file:
        with gzip.open(target, 'wb') as tar_file:
            shutil.copyfileobj(src_file, tar_file)
    os.remove(source)

handler = logging.handlers.TimedRotatingFileHandler(
    os.path.join(log_dir, log_file),
    when='M',
    interval=1,
    backupCount=12,
    delay=True
)
handler.setFormatter(formatter)
handler.rotator = compress_log
logger.addHandler(handler)

# 配置安全连接
session = requests.Session()
if args.cert:
    session.verify = args.cert
else:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session.verify = False

# 构建API URL
api_url = f"https://{args.ip}:{args.port}/ws/v1/cluster/apps?state=RUNNING"

# 辅助函数：获取应用信息
def get_apps():
    try:
        response = session.get(api_url, auth=(':', ''), timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('apps', {}).get('app', [])
    except Exception as e:
        logger.error(f"获取应用列表失败: {str(e)}")
        return []

# 辅助函数：终止单个应用
def terminate_app(app_id):
    try:
        kill_cmd = ["yarn", "application", "-kill", app_id]
        process = subprocess.Popen(kill_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate(timeout=60)

        if process.returncode == 0:
            logger.info(f"成功终止应用: {app_id}")
            return True
        else:
            logger.error(f"终止应用失败: {app_id} - {stderr.decode().strip()}")
            return False
    except Exception as e:
        logger.error(f"终止应用异常: {app_id} - {str(e)}")
        return False

def main():
    logger.info(f"脚本启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"筛选参数: 运行超过{args.hours}小时的应用")

    # 获取所有应用
    apps = get_apps()
    if not apps:
        logger.info("没有找到运行中的应用")
        return

    # 当前时间
    now_time = datetime.now()
    logger.info(f"当前时间: {now_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"初始应用数量: {len(apps)}")

    # 筛选应用
    filtered_apps = []
    for app in apps:
        # 修正1: 队列过滤逻辑
        queue = app.get('queue', '')
        if not queue.startswith('hetu_adhoc_'):
            continue

        # 排除Flink应用
        app_type = app.get('applicationType', '')
        if app_type == 'Apache Flink':
            continue

        # 修正2: 时间计算
        started_time = app.get('startedTime', 0)
        if started_time == 0:
            continue

        # 转换时间戳(毫秒转秒)
        start_dt = datetime.fromtimestamp(started_time / 1000)
        time_diff = now_time - start_dt

        # 检查运行时间
        if time_diff < timedelta(hours=args.hours):
            continue

        # 排除特定名称应用
        app_name = app.get('name', '')
        if '-coordinator' in app_name or '-worker' in app_name:
            continue

        # 添加到结果列表
        allocated_mb = app.get('allocatedMB', 0)
        app_info = {
            'id': app['id'],
            'name': app_name,
            'user': app.get('user', ''),
            'queue': queue,
            'startedTime': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'elapsed_hours': time_diff.total_seconds() / 3600,
            'allocatedGB': allocated_mb / 1024 if allocated_mb else 0,
            'allocatedVCores': app.get('allocatedVCores', 0),
            'runningContainers': app.get('runningContainers', 0)
        }
        filtered_apps.append(app_info)

        # 记录应用信息
        logger.info(f"符合终止条件的应用: ID={app['id']}, 名称={app_name}")
        logger.info(f"  用户: {app_info['user']}, 队列: {queue}")
        logger.info(f"  启动时间: {app_info['startedTime']}, 已运行: {app_info['elapsed_hours']:.2f}小时")
        logger.info(f"  资源: {app_info['allocatedGB']:.2f}GB, {app_info['allocatedVCores']}vCores, {app_info['runningContainers']}容器")
        logger.info("-" * 80)

    # 终止应用
    if filtered_apps:
        app_ids = [app['id'] for app in filtered_apps]
        logger.info(f"准备终止 {len(app_ids)} 个应用...")

        # 优化: 并行终止应用
        success_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(terminate_app, app_ids))
            success_count = sum(results)

        logger.info(f"应用终止完成: 成功 {success_count}/{len(app_ids)}")

        # 状态验证
        logger.info("等待30秒后验证应用状态...")
        time.sleep(30)

        # 检查应用状态
        try:
            remaining_apps = get_apps()
            remaining_ids = [app['id'] for app in remaining_apps]

            # 修正3: 状态检查逻辑
            still_running = [app_id for app_id in app_ids if app_id in remaining_ids]

            if still_running:
                for app_id in still_running:
                    # 查找应用当前状态
                    app_status = next(
                        (app['state'] for app in remaining_apps if app['id'] == app_id),
                        '未知'
                    )
                    logger.warning(f"应用 {app_id} 仍存在，状态: {app_status}")
            else:
                logger.info("所有目标应用已成功终止")

        except Exception as e:
            logger.error(f"状态验证失败: {str(e)}")
    else:
        logger.info("没有需要终止的应用")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("脚本被用户中断")
    except Exception as e:
        logger.exception(f"未处理的异常: {str(e)}")
    finally:
        logger.info("脚本执行结束")