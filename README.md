通过命令 java -jar db2file.jar 来运行本jar包。  
通过修改p.properties文件内容来修改相关参数。  
* 其中：  
  1. ip字段为数据库ip；
  2. port字段为数据库端口；
  3. user字段为可访问数据库的用户名；
  4. password字段为用户名对应的密码；
  5. db_name字段为关系模式所在的库；
  6. tables_name字段为需要导出内容的表名，其中表名之间用英文逗号“,”分隔；
  7. output_path字段为生成结果文件的文件夹地址；
  8. separator字段为数据属性间的分隔符，值为“\\b”和“space”时用空格符分隔，为“\\t”和“tab”时用制表符分隔，为“comma”或“,”以及其他值时用英文逗号分隔；
	store_null_data字段表示是否保存含有空值的数据，值为“n”或“no”时不保存，为“y”或“yes”以及其他值时保存；
	get_way字段表示导出表中的哪些值，值为all时导出所有数据，为today时导出当天数据，以“yyyy-MM-dd”格式输入日期时导出该天数据，以“yyyy-MM-dd,yyyy-MM-dd”格式输入日期时导出两天之间的数据，其中前一个日期应早于后一个日期，为manual时导出执行script字段的sql语句后的查询结果；
	script字段为用户自定义的sql语句，当get_way字段值为manual时有效；

* 当get_way字段不为manual时，输出文件以 表名_日期.txt 的形式存储在output_path的文件夹中。否则以manual_日期.txt的形式存储在output_path的文件夹中。
