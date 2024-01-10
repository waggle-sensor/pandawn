import sage_data_client
import subprocess

# this follows the https://sagecontinuum.org/docs/tutorials/accessing-data#accessing-large-files-ie-training-data
df = sage_data_client.query(
    start="2024-01-10 00:00:00.000000000+00:0",
    #end="2023-12-20 00:00:00.000000000+00:0",
    filter={
        "name": "upload",
        "vsn": "****", # replace with DAWN node ID line W022 W01B etc. 
        "task": "panda-rosbag-radenv",
    },
)

df.value.to_csv("urls.txt", index=False, header=False)
#this needs username and password shared out-of-band
subprocess.check_call(["wget", "--user=****", "--password=*****", "-r", "-N", "-i", "urls.txt"])

