# Getting Up and Running

1. Checkout this repo

2. Put a csv file in src/main/resourcs/transactions.csv, with this header line
 "Date","Account","Description","Category","Tags","Amount" and some dummy data. The Date/Tags fields can be empty strings,
since the code doesn't use them currently anyways

3. Alternatively, you can export your own financial data from PersonalCapital - their CSV is in this format

4. Run `sbt run` on the command line OR Run `sbt eclipse` and run `Driver.scala` from your IDE
