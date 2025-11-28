grammar MultipleValues;

field : content;

content : valData | content VALSEPCHAR content;

valData : value (METASEP metaData)*;

value : anyContent;

metaData : anyContent;

anyContent : ANYCHAR+;

VALSEPCHAR : SPACE*'<##>'SPACE*;

METASEP : SPACE*'<###>'SPACE*;

ANYCHAR : .;

SPACE : ' ';
