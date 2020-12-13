"""Custom type definitions to be used in filecluster package."""
from typing import NewType
import pandas as pd

MediaDataframe = NewType("Images", pd.DataFrame)
ClustersDataframe = NewType("ClustersDataframe", pd.DataFrame)
