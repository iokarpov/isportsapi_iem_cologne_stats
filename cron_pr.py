from crontab import CronTab

cron = CronTab(user='iokar')
job = cron.new(command='python3 /home/iokar/Documents/py/pari_hw/luigi_pr.py create_pdf_with_IEM_Cologne --local-scheduler')
job.hour.on(9)

cron.write()

