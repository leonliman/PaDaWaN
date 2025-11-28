grammar TextContent;

content	: expression+ EOF;

expression
 : '(' expression ')'			# complexExpression     
 | queryPart					# queryElemExpression
 | expression AND expression	# booleanAnd
 | expression OR expression		# booleanOr
 ;


queryPart	: near | regexQuery | words | phrase;

words	: word+;

word	: WORD | NUMBER;

near	: '[' distance? sequence? words ']';

regexQuery	: regex regexCondition? outputdefinition? ;

regexCondition : '[' reference numericCondition? ']' ;

numericCondition : numericOperator bound;

numericOperator	: NUMERIC_OPERATOR ;

//outputdefinition: reference ;
outputdefinition: .*?  ;

reference :  group  | namedEntity ;

namedEntity : ZAHL;

regex	: REGEX;

phrase	: PHRASE;

bound		: NUMBER;
group		: DOLLAR NUMBER;
distance	: NUMBER;
sequence	: EQAULS;


ZAHL : 'ZAHL';

AND	: 'AND' | 'And' | 'and' | 'UND' | 'Und' | 'und' | '&&' | '&' | ','  ;

OR		: 'OR' | 'Or' | 'or' | 'ODER' | 'Oder' | 'oder' | '||' | '|' ;
EQAULS	: '=';
SBO 	: '[';
SBC		: ']';
SLASH	: '/';

NUMBER	:  DIGIT+ ;

NUMERIC_OPERATOR : '<' | '<=' | '>' | '>=' | '=' ;

WORD	:  WordChar+	;

REGEX	: '/' ('\\/' | ~( '\r' | '\n' ))+? '/';

PHRASE : '"' (~('\\' | '\r' | '\n'))*? '"';

DOLLAR 	: '$' ;


fragment
WordChar	: ~(' '| '\r' | '\n' |  ',' |  '(' | ')' | '[' | ']' | '/' | '=' | '$' | '"' | '{' | '}' ) ;

fragment
DIGIT	: [0-9];

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ -> skip ;
