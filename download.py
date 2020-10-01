
# Databricks notebook source
# COMMAND ----------
import io
from typing import Union
from IPython.display import HTML, SVG
import base64
import json

# COMMAND ----------
def download_button(b64_buffer, filename:str):
  display(HTML(f'<a href=\"data:application/octet-stream;base64,{b64_buffer}\" download={filename}><button>Download a file</button></a>'))


def dict_to_json_buffer(dict: dict):
  buf= io.StringIO()
  print(json.dumps(df.to_dict(),indent=2),file=buf)
  bufo = base64.b64encode(buf.getvalue().encode('utf8')).decode()
  buf.close()
  return bufo


def encode_fig_to_buffer(plot, format="SVG"):
"""
plot: plt.fig or plotly plot
  plot to turn into a buffer
format: str
  "SVG" or "PNG"
"""
  buf= io.StringIO()
  plot.get_figure().savefig(buf, format="SVG")
  bufo = base64.b64encode(bytes(buf.getvalue(), 'ascii')).decode()
  buf.close()
  return bufo
 
# COMMAND ----------
def download_json(dict: dict, filename: str):
  buf = dict_to_json_buffer(dict)
  download_button(buf, filename)


def download_image(plot, filename: str, format="SVG"):
  buf = encode_fig_to_buffer(plot, format="SVG")
  download_button(buf, filename)
