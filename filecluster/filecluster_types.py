"""Custom type definitions to be used in filecluster package."""
from typing import NewType
import pandas as pd

MediaDataFrame = NewType("MediaDataFrame", pd.DataFrame)
ClustersDataFrame = NewType("ClustersDataFrame", pd.DataFrame)
