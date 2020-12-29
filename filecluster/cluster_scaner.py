"""Module with function to scan the directories and obtain existing cluster info."""
# TODO: KS: 2020-12-28: move contents to update_cluster
import os
import re
from configparser import ConfigParser
from datetime import datetime
from pathlib import PosixPath, Path
from typing import Dict, Union, List, Tuple, Optional

from filecluster.configuration import INI_FILENAME
