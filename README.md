# Virtual Meter Aggregation

Suppose you have the following graph related situation:

> A customer is working on creating aggregates for their virtual meters. 
> These virtual meters can connect to physical meters or other virtual meters. 
> They are looking at looping to aggregate each level, but it seemed to me more 
> like a graph problem. Is there an easy way to sum readings from physical's 
> and keep that aggregate going through each vertices. We would only know the 
> physical readings to start. The attached image is what I was thinking could 
> happen by filling out the aggregates at it virtual.

This notebook provides an example of how to implement the desired aggregation
using the [message passing API](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.lib.html) in [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) on [Apache Spark](https://spark.apache.org/).

Open the notebook to see the example.

## License

Copyright 2023 Databricks Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
