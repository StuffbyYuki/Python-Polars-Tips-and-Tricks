import polars as pl

#define and register your custom functionality
@pl.api.register_expr_namespace('custom')
class CustomStringMethodsCollection:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def to_title_case(self) -> pl.Expr:
        convert_to_title = (
            pl.element().str.slice(0, 1).str.to_uppercase() 
            + 
            pl.element().str.slice(1).str.to_lowercase()
            )
        
        converted_elements = (
            self._expr
            .str.split(' ')
            .arr.eval(convert_to_title)
            .arr.join(separator=' ')
            )
        return converted_elements

# see if this works
df = pl.LazyFrame(
    {'Name': ['mike mikEMiKe', 'SaRaH SarAhSarah MIKe', 'your name']}
)

print(
    df.with_columns(
        pl.col('Name').custom.to_title_case()
    )
    .collect()
)
'''
output:
shape: (3, 1)
┌───────────────────────┐
│ Name                  │
│ ---                   │
│ str                   │
╞═══════════════════════╡
│ Mike Mikemike         │
│ Sarah Sarahsarah Mike │
│ Your Name             │
└───────────────────────┘
'''