# Transfer Learning Example

This project in Cloudera Machine Learning (CML) shows you how you can take a AI training workload (in this case using Tensorflow and Keras) and gain massive speed improvements by leveraging extra computational resources in the cloud.

## Usage

In order to track the performance of the model, we're going to use CML experiments. Comment out the following code when running outside of CML-Experiments.

```python
import cdsw
cdsw.track_metric('Train Time', str(totalTime))
```
Now all you have to do is create a new experiment, point it to the Main.py code, and throw as many resources as you'd like to see how it affects the speed of model training!