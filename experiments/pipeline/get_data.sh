# http://archive.ics.uci.edu/ml/datasets/wine+quality
INPUT=$(dstools env path.input)

mkdir $INPUT/raw

curl http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv \
    -o $INPUT/raw/red.csv
curl http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv \
    -o $INPUT/raw/white.csv
curl http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality.names \
    -o $INPUT/raw/names