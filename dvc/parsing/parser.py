import pyparsing as pp
from dataclasses import dataclass

PERIOD = "."


def transform_brackets(s, loc, tokens):
    return f"{PERIOD}{tokens[0]}"


@dataclass
class Expression:
    inner: str


LBRACK = "["
RBRACK = "]"
DOLLAR = "$"
LBRACE = "{"
RBRACE = "}"
SUBST_OPEN = "${"
SUBST_CLOSE = RBRACE

lbracket = pp.Suppress(LBRACK)
rbracket = pp.Suppress(RBRACK)

word = pp.CharsNotIn(f"{PERIOD}{LBRACK}{RBRACK}{LBRACE}{RBRACE}")

index = pp.Suppress(LBRACK) + word + pp.Suppress(RBRACK)
index.setParseAction(transform_brackets)
attr = pp.Literal(PERIOD) + word
inner = word + pp.ZeroOrMore(attr | index)
expr = pp.Combine(
    ~pp.PrecededBy("\\")
    + pp.Suppress(SUBST_OPEN)
    + inner
    + pp.Suppress(SUBST_CLOSE)
).setParseAction(Expression)

text = pp.SkipTo(
    expr | pp.Suppress(pp.lineEnd), include=True
).leaveWhitespace()
parser = text | expr
