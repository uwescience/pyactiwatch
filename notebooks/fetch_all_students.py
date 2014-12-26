
import sqlshare
from bokeh.plotting import *
import numpy as np

conn = sqlshare.SQLShare()

TOOLS="pan,wheel_zoom,box_zoom,reset,previewsave"

students = conn.execute_sql("SELECT DISTINCT [Student Code] as studentid FROM [angelak1@washington.edu].[Grades]")

for row in students["sample_data"]:
  studentid = row[0]
  sql = "SELECT * FROM [angelak1@washington.edu].[Avg_weekday_and_weekend_perstudent] WHERE studentid = %s" % studentid
  dat = conn.execute_sql(sql)
  print studentid, dat["exec_time"], dat["rows_total"]
  sfile = open("%s.csv" % studentid,"w")
  names = [d["name"] for d in dat["columns"]]
  sfile.write(",".join(names) + "\n")
  for dayrow in dat["sample_data"]:
   sfile.write(",".join([str(x) for x in dayrow]) + "\n")
  sfile.close()

