增加一个批量更新数据库脚本，将需要更改的id导出来拼接成sql语句，并写入到单个文件，利用pt-fifo-split工具切割分批执行，以降低对主从数据库的影响。