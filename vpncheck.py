import time
import os
import requests

def LogFileMSG(level,WriteLogMessage):
    #print(WriteLogMessage)
    #logfilename = time.strftime("Log%Y%m%d.txt",time.localtime())
    logfilename = time.strftime("Log%Y%m.txt", time.localtime())
    logtime1=time.strftime("%Y/%m/%d %H:%M:%S ",time.localtime())
    MsgLevel=("INFO ","WARN ","ERROR ","FATAL ","DEBUG ")
    try:
        logf = open(logfilename, 'a')
        logf.write(logtime1 + '_' + MsgLevel[level] + '_' + WriteLogMessage + '\n')
        logf.close()
    except FileNotFoundError:
        print("FileNotFound")
        logf = open(logfilename,"w")
        logf.write(logtime1 + '_' + MsgLevel[level] + '_' + WriteLogMessage + '\n')
        logf.close()
    except IsADirectoryError:
        print("IsADirectory")

def runserverlogin():
    ip = requests.get('https://api.ipify.org').text
    if ip == "61.218.59.85":
        LogFileMSG(0,"RUN ServerLogin")
        os.system('./serverlogin')
    else:
        time.sleep(3)
        runserverlogin()

def connectVPN():
    os.system('scutil --nc start "VPN (L2TP)" --secret JvdiamondTech@33')
    # os.system('scutil --nc start "OfficeVPN (L2TP)" --user ck --password XXXXXX --secret JvdiamondTech@33')
    time.sleep(1)
    os.system('scutil --nc list')
    runserverlogin()

def checkvpnconfig():
    #config = os.system('scutil --nc status "OfficeVP"')
    config = os.system('scutil --nc status "VPN (L2TP)"')
    if config == 256:   #NO config
        print("NO VPN config, Please Create VPN Config,first")
    elif config == 0:   #config exist , but disconnct
        print("config exist , but disconnct")
        connectVPN()
    else:
        print("CCCCC")

def get_ip():
    ip = requests.get('https://api.ipify.org').text
    #61.218.59.85  # OFFICE VPN
    #125.228.238.157   # 1F OFFICE
    if ip == "61.218.59.85":
        print("you are in VPN")
        runserverlogin()
    elif ip == "125.228.238.157":
        print("you are in 1F office, START vpn now..")
        checkvpnconfig()
    elif ip == "114.32.10.1":
        print("you are in CK Home, START vpn now..")
        checkvpnconfig()
    else:
        print("I don't who you are, START vpn now..")
        checkvpnconfig()
    #scutil --nc stop "OfficeVPN (L2TP)"
    #scutil --nc start "OfficeVPN (L2TP)" --secret JvdiamondTech@33
    #print(ip)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    os.system('cat ./serverlogin.txt')
    time.sleep(2)
    get_ip()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/