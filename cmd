source okx-env-3.8/bin/activate
cd ~/python-okx/python-okx
git fetch origin
git pull origin dev_2505
python myWork/dca/exec.py

--开始的命令
pkill -f "python myWork/dca/exec.py"  # 终止所有运行 exec.py 的进程
pkill -f "bash -c source okx-env-3.8"  # 终止所有运行该脚本的 bash 进程
nohup bash -c "source okx-env-3.8/bin/activate && \
cd python-okx/python-okx && \
git fetch origin && \
git pull origin dev_2505 && \
python myWork/dca/exec.py" &


查看进程
ps -ef | grep exec.py


pkill -f "python myWork/dca/exec.py"  # 终止所有运行 exec.py 的进程
pkill -f "bash -c source okx-env-3.8"  # 终止所有运行该脚本的 bash 进程