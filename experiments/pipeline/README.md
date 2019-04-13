# pipeline example

```bash
DB=$(dstools env db)
INPUT=$(dstools env path.input)
HOME=$(dstools env path.home)

# get raw data
bash get_data.sh

# sample
python sample.py

# upload sample/red.csv
csvsql --db $DB --tables red --insert "$INPUT/sample/red.csv"  --overwrite

# upload sample/white.csv
csvsql --db $DB --tables white --insert "$INPUT/sample/white.csv"  --overwrite

# create view with both tables
psql $DB -f $HOME/sql/create_view.sql

# select features and add label
psql $DB -f $HOME/sql/create_dataset.sql

# create trainng set
psql $DB -f $HOME/sql/create_training.sql

# create testing set
psql $DB -f $HOME/sql/create_testing.sql
```