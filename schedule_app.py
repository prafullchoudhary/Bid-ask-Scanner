import schedule
import time
import os

def job():
    os.system("python app.py")

schedule.every().day.at("09:15:00").do(job)
# schedule.every().minute.at(":00").do(job)

# Keep the script running and execute the scheduled job
while True:
    schedule.run_pending()
    time.sleep(30)