grammar QuickSearch;


parse	: expression EOF ;
 
input	: query?  text?  EOF;

textAttributeInput	: IN catalogEntry? queryTokens? ;

queryAttributeOnly	: queryAttribute EOF;					

query : expression conjunction;

expression
 : '(' expression ')'			# parenExpression  
 | '{' expression '}'			# groupExpression
 | queryAttribute				# queryAttribute1 
 | expression AND expression	# booleanAnd
 | expression OR expression		# booleanOr
 ;
 

queryAttribute	: negation? (boolAttribute | numericAttribute | textAttribute) alias? ;

boolAttribute	: catalogEntry reductionOperator? sucessor?;

numericAttribute: catalogEntry reductionOperator?  numericCondition ;

textAttribute	: in catalogEntry plus? queryTokens ; 

catalogEntry	: WORD ;

queryTokens		: STRING_LITERAL | queryTextTokens+ ;

numericCondition	: lowerBound dots upperBound 				# closedInterval
					| numericOperator bound							# openInterval
					| dots? lowerBound (dots numberOrDate)* dots? # manyIntervals
					| number negation number						# yearPeriod
					;


negation	: NEGATION;

alias		: AS aliasName;

sucessor	: SUCCESSOR;

conjunction	: AND | OR ;

reductionOperator	: REDUCTION_OPERATOR;

plus	: PLUS; 

in		: IN;

dots: DOTS;

lowerBound	: numberOrDate;

upperBound	: numberOrDate;

bound		: numberOrDate;

number			: NUMBER; 

numberOrDate	: NUMBER | DATE;

numericOperator	: NUMERIC_OPERATOR ;
 
queryTextTokens: WORD | NUMBER ;

aliasName	: WORD |  NUMBER ;

text: (IN | WORD | COMMA | STRING_LITERAL | AT | NUMERIC_OPERATOR | DOTS | NUMBER | REDUCTION_OPERATOR | NEGATION | PLUS)*;



// Lexer Rules

NEGATION: MINUS;

PLUS	: '+' ;

IN	:	'in:' | 'In:' | 'IN:' ;	

DOTS	: '...' ;

AS	: 'AS' | 'as' | 'ALS' | 'Als' | 'als';

AND: 'AND' | 'And' | 'and' | 'UND' | 'Und' | 'und' | '&&' | '&' | ','  ;

OR: 'OR' | 'Or' | 'or' | 'ODER' | 'Oder' | 'oder' | '||' | '|' ;

SUCCESSOR : 'Nachfolger' | 'nachfolger' | 'NACHFOLGER' ;

DATE	: DIGIT DIGIT?  '.' DIGIT DIGIT? '.' DIGIT DIGIT (DIGIT DIGIT)?
		| DIGIT DIGIT (DIGIT DIGIT)? MINUS DIGIT DIGIT? MINUS DIGIT DIGIT?
		;

NUMBER: '-'? DIGIT+ ((',') DIGIT+ )? ;
  
NUMERIC_OPERATOR : '<' | '<=' | '>' | '>=' | '=' ;

REDUCTION_OPERATOR	: 'min' | 'MIN' | 'max' | 'MAX' | 'first' | 'FIRST' | 'last' | 'LAST' ;

WORD	: WordStartChar (
		{!((""+(char)_input.LA(1)+(char)_input.LA(2)+(char)_input.LA(3)).equals("...")
		  || (getText().equals("in:")||getText().equals("In:")||getText().equals("IN:")))}? 
		 WordChar
		 )* 
		;


STRING_LITERAL : '\'' (~( '\r' | '\n'))*? '\'';

fragment
MINUS	: '-';

fragment
DIGIT	: [0-9];

fragment
WordStartChar	: ~('-' | '.' | ' '| '\r' | '\n' |  ',' |  '@' | '<' | '>' | '=' | '(' | ')' | '{' | '}') ;

fragment
WordChar	:  ~(' '| '\r' | '\n' |  ',' |  '@' | '<' | '>' | '=' | '(' | ')' | '{' | '}') ;


 
WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ -> skip ;