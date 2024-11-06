# https://myaccount.google.com/apppasswords
# xkrm ybgo hagw qodg

import smtplib
server=smtplib.SMTP('smtp.gmail.com',587)
server.starttls()
server.login('vsaranyadurai@gmail.com','xkrm ybgo hagw qodg')
server.sendmail('vsaranyadurai@gmail.com','vsaranyadurai@gmail.com','i am from py')
print("mail sent")
