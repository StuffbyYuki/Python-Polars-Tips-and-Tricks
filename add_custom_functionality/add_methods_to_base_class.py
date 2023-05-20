import polars as pl

def to_title_case(self) -> pl.Expr:
        convert_to_title = (
            pl.element().str.slice(0, 1).str.to_uppercase() 
            + 
            pl.element().str.slice(1).str.to_lowercase()
            )
        
        converted_elements = (
            self
            .str.split(' ')
            .arr.eval(convert_to_title)
            .arr.join(separator=' ')
            )
        return converted_elements

pl.Expr.to_title_case = to_title_case

df = pl.LazyFrame(
    {'Name': ['mike mikEMiKe', 'SaRaH SarAhSarah MIKe', 'your name']}
)

print(
    df.with_columns(
        pl.col('Name').to_title_case()
    )
    .collect()
)
'''
output:
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