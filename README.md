# recomigo
Article recommendation implemented in Spark

## Usage

### Installation

```
brew install python3
brew install apache-spark
export PYSPARK_PYTHON=python3
pip install -r requirements.txt
```

I would also recommend to lower logging level from INFO to WARN:
```
# In <apache-spark dir>/libexec/conf
cp log4j.properties.template log4j.properties
vi log4j.properties
```
Change "INFO" in `log4j.rootCategory=INFO, console` to "WARN"

### Running

```
# Run the application
spark-submit main.py
```

