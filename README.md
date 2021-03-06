# Davisbase-SQL DATABASE ENGINE

###A java-based SQL databse engine which support common SQL syntax (syntax checker embedded):

#### Note: A password-based encryption module embedded. 
####Default setting:

##### Username: root
##### Password: root

#### CREATE DATABASE

> #### CREATE DATABASE database_name;

#### CREATE TABLE

> #### CREATE TABLE table_name (attrName dataType Constraints_List);

#### INSERT INTO

> #### INSERT INTO table_name (Attr_List) VALUES (Value_List);

> or

> #### INSERT INTO table_name VALUES (Value_List);

#### UPDATE (where clause can be applied to either key or non-key attribute)

> #### UPDATE table_name SET attr1=val1,attr2=val2,... (WHERE condition);

#### DROP TABLE

> #### DROP TABLE table_name;

#### SELECT-FROM-WHERE (where clause can be applied to either key or non-key attribute)

> #### SELECT * FROM table_name (WHERE condition);

> or

> #### SELECT attr_List FROM table_name (WHERE condition);

#### USE

> #### USE database_name;

##File Format:

####.tbl is the table file
- databaseHeader:

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/file%20format%201.JPG)

##### 0x0001: # of byte for magic header

##### 0x0002-0x0013: magic header


##### 0x0014-0x0015: page size


##### 0x0016: most recent version of DBMS that modifies the file

 
##### 0x0017: most recent version of DBMS that read the file


##### 0x0018: reserved unused space at the end of each page


##### 0x0019: Maximum embedded payload fraction (reserved, currently 64)


##### 0x001A: Minimum embedded payload fraction (reserved, currently 32)


##### 0x001B: Leaf payload fraction (reserved, currently 32)


##### 0x001C-0X001F: file change counter


##### 0x0020-0x0023: file size in pages


##### 0x0024-0x0027: text encoding schema


##### 0x0028-0x003B reserved space


##### 0x003C-0X003F: Davisbase version number


##### 0x0040-0x0043: the version-valid-for number

- page header:

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/file%20format%202.JPG)

##### 0x00:   0x05 - table interior node; 0x0D - table leaf node

##### 0x01-0x02: start of frist free block in a page (0 if no free block)

##### 0x03-0x04: number of cells in a page

##### 0x05-0x06: start of cell content area

##### 0x07: number of free fragments in a page

##### 0x08-0x0B: right page pointer

##### 0x0C-: cell pointer array

- cell:

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/file%20format%203.JPG)

#### leaf page:

##### 0x00: delete marker

##### 0x01-0x02: payload size

##### 0x03-0x06: key value

##### 0x07-0x08: payload header size

##### 0x09- : attribute type code series

##### following: value of each attribute

#### interior page:

##### 0x00: delete marker

##### 0x01-0x04: key value

##### 0x05-0x08: page pointer

- Record Format:

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/file%20format%204.JPG)

## Examples:

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%201.JPG)

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%202.gif)

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%203.gif)

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%204.gif)

![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%205.gif)
![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%207.JPG)
![](https://github.com/AlenUbuntu/Davisbase---SQL-DATABASE-ENGINE/blob/master/show%209.gif)

