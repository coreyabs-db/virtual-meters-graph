# Databricks notebook source
# MAGIC %md
# MAGIC # Virtual Meter Aggregation
# MAGIC
# MAGIC Suppose you have the following graph related situation:
# MAGIC
# MAGIC > A customer is working on creating aggregates for their virtual meters. 
# MAGIC > These virtual meters can connect to physical meters or other virtual meters. 
# MAGIC > They are looking at looping to aggregate each level, but it seemed to me more 
# MAGIC > like a graph problem. Is there an easy way to sum readings from physical's 
# MAGIC > and keep that aggregate going through each vertices. We would only know the 
# MAGIC > physical readings to start. The attached image is what I was thinking could 
# MAGIC > happen by filling out the aggregates at it virtual.
# MAGIC
# MAGIC This notebook provides an example of how to implement the desired aggregation
# MAGIC using the [message passing API](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.lib.html) in [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) on [Apache Spark](https://spark.apache.org/).

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to import some pyspark and graphframes related packages.
# MAGIC
# MAGIC We'll also ignore a few warnings that come up when working with graphframes that we can safely ignore.
# MAGIC
# MAGIC Finally, for reference, we display an image of the sample graph we're going to explore.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM

from IPython.display import Image

from warnings import filterwarnings

filterwarnings('ignore', 'DataFrame.sql_ctx is an internal property')
filterwarnings('ignore', 'DataFrame constructor is internal')

Image(filename='meters.png', retina=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we define the graph as it was depicted in the image above.
# MAGIC
# MAGIC Note that the id's are specified as simple integer id's. This allows 
# MAGIC the spark data type to be long, which is ideal for graphframes. It will
# MAGIC work with other types, but internally it will incur a bit of overhead 
# MAGIC to convert it for you. If it's already a long, it doesn't need to.

# COMMAND ----------

vertices_list = [
    {"id": 1, "type": "physical", "reading": 2},
    {"id": 2, "type": "physical", "reading": 3},
    {"id": 3, "type": "physical", "reading": 10},
    {"id": 4, "type": "physical", "reading": 10},
    {"id": 5, "type": "virtual", "reading": None},
    {"id": 6, "type": "virtual", "reading": None},
    {"id": 7, "type": "virtual", "reading": None},
    {"id": 8, "type": "virtual", "reading": None},
]

edges_list = [
    {"src": 1, "dst": 5},
    {"src": 2, "dst": 6},
    {"src": 3, "dst": 6},
    {"src": 4, "dst": 7},
    {"src": 6, "dst": 8},
    {"src": 7, "dst": 8},
]

vertices = spark.createDataFrame(vertices_list)
edges = spark.createDataFrame(edges_list)
graph = GraphFrame(vertices, edges).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Next we define the method containing our main aggregate messages routine.
# MAGIC
# MAGIC The messages in this case are integers.

# COMMAND ----------

def compute_sums(graph: GraphFrame, max_iter: int) -> DataFrame:
    
    # initialize working vertices
    v = (
        graph.vertices
        .withColumn('total', F.coalesce(F.col('reading'), F.lit(0)))
        .withColumn('increment', F.col('total')))
    g = GraphFrame(AM.getCachedDataFrame(v), graph.edges)
    
    # expand the increment fringe one layer at a time
    for i in range(max_iter):
        # send and aggregate messages telling each parent of the increments
        agg = (
            g.aggregateMessages(
                F.sum(AM.msg).alias('increment'), 
                sendToDst=AM.src['increment'])
            .filter(F.col('increment') > 0))
        
        # exit early if there are no more increments to communicate
        if agg.isEmpty():
            break

        # incorporate the aggregate increments for this layer and 
        # propogate the received increment to the next layer
        v = AM.getCachedDataFrame(
            g.vertices.join(agg, on='id', how='left_outer')
            .withColumn('total', 
                F.when(agg.increment.isNull(), g.vertices.total)
                .otherwise(agg.increment + g.vertices.total))
            .withColumn('new_increment', agg.increment)
            .drop('increment').withColumnRenamed('new_increment', 'increment'))
        g = GraphFrame(v, g.edges)
    
    # return the final vertices and their totals
    return g.vertices.select('id', 'total')


# COMMAND ----------

display(compute_sums(graph, 5))

# COMMAND ----------

# MAGIC %md
# MAGIC And that's it! As you can see, the algorithm correctly computed the sums for each of the nodes in the graph.
