HTMLPARSER_HOME=/home/terry/htmlparser1_6/lib

compile : getText.java
	javac -classpath $(HTMLPARSER_HOME)/htmlparser.jar:$(HTMLPARSER_HOME)/htmllexer.jar:$(HTMLPARSER_HOME)/thumbelina.jar getText.java

run : compile
	java getText

