# 实用指南

## 环境
### hadoop-2.7.3、spark-2.3.1-bin-hadoop2.7、python2.7

## 目录结构说明
###  _[目录结构说明](./tree_proj.MD)_

## 添加新代码
- 统一在 biz 目录下添加需要清洗的代码。
- 清洗原始数据，放在clean_raw_data下，建立自己的目录，在新建的目录下添加py文件
- 清洗中间数据表，放在 middle_layer 下，建立自己的目录，在新建的目录下添加py文件
- 构建自己的特征，放在 feature 下，建立自己的目录，在新建的目录下添加py文件

## 配置
- 所有代码需要执行的class需要添加到 `conf/all_task_conf.py` 文件中。

## 运行
- analyse.py 分析计算图脚本
- deploy.py 部署脚本
- run.py  部署时产生的脚本，用于指定单一运算，还是跑所有任务， 该脚本有 `deploy.py` 在部署时自动生成
- setup.py 安装依赖的脚本。添加自定义清洗方法是需要修改此文件。

### 检查表依赖关系
- 可以生成依赖的表关系
```
python analyse.py
```

### 添加安装文件
- 如果在清洗过程中，在rdd的map、reduce、filter的方法中用到了自定义的方法，需要将方法添加到 `setup.py` 中。并在正式运行前执行运行安装环境的代码
```
python deploy.py -a set_env
```

### 运行方式
#### 单独运行一个class
- 可在调试时使用
- `all_task_conf.py` 文件中的 `need_to_run` 设置为`False`，不影响单模式下，单个class的运行。
```
python deploy.py -m single -cls $YOUR_CLASS_TO_RUN
```

#### 运行所有任务
- 目前是串行执行 `conf/all_task_conf.py` 所配置的class。
- 开发者可以在 `conf/all_task_conf_sub` 目录下面 新建自己的类别，然后在 `all_task_conf.py` 引入
- 将运行所有设置为`True`的任务，所以在使用的时候要小心。建议提交的代码都设置为`False`。
```
python deploy.py
```

## 一般开发流程
- pull 代码， 运行 `analyse.py`， 生成 dag.gv.pdf 文件， 打开该文件。选择合适的依赖表，找到该依赖表的class。该依赖表的class就是其他人生成该表的过程。
- 拷贝别人class中生成表的结果表的 `TableInfo` 。作为自己的依赖表。
- 在biz合适的目录下创建文件，继承 `SparkSessionUtils` 类，补充 `set_table_info` 方法和 `run_task` 方法，编写清洗逻辑。
- 在 `conf/all_task_conf_sub` 或 `conf/all_task_conf_sub_time` 补充需要执行的class。 如果是新增的任务配置文件还需要在 `conf/all_task_conf.py` 加入class。



## 数据仓库的table
- _[表](./tableInfo.MD)_
