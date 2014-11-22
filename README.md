DocumentAnalysis
================

Wikipedia document analysis using Hadoop

***
Map的每个输入是XML文档的
	<page>
标签到
	</page>
标签。其中key没有意义，value就是这两个标签（包括标签本身）的值，可以通过
	.toString()
方法转化为字符串进行下一步处理
***

src/documentParser
---
	TextParser.java
正则表达式处理String，能够去除大部分标点符号，需要补全

	XMLHandler.java
SAX流形式处理XML格式的字符串
