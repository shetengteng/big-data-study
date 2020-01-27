- executor memory和executor cores如何设置?

  - 默认值在spark-env.sh脚本中有写明

  - memory默认1G

  - driver memory 默认1G

  - cores默认1个

  - 在20台机器：2160G（`108*20`），720线程cpu（`36*20`）

    ![](../xx.project/07-在线教育项目/img/8.png) 

    ![](../xx.project/07-在线教育项目/img/9.png) 

    

    - 每台128G内存，40核cpu，8T硬盘
    - 36线程给计算任务，4线程给其他任务
    - 108G可以分配任务

  

