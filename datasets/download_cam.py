import sage_data_client
import subprocess

# start pulling data from 2023/09

df = sage_data_client.query(
    start="2024-01-01 00:00:00.000000000+00:0", 
    end="2024-01-02 00:00:00.000000000+00:0",
    filter={
        "name": "upload",
        "vsn": "W072",
        "task": "panda-camera-sched",
    },
)

# write values (urls)
df.value.to_csv("urls.txt", index=False, header=False)

# download everything in urls file
subprocess.check_call(["wget", "--user=***", "--password=***", "-r", "-N", "-i", "urls.txt"])

