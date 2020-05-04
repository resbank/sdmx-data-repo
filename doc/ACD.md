## Apache Commons Daemon

Download the ACD binaries `commons-daemon-1.2.1-bin-windows.zip` and extract them to a suitable folder. Assuming you are using a 64 bit version of the JVM add the 64 bit executable `.\amd64\procrun.exe` to your system path.

### Make the necessary changes to `sparta-app.application` for `procrun`

Remove the `-main` method and replace it with the follwing start and stop methods

```
(def system (app-system (config)))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system component/stop))

(defn -start [args] (start))

(defn -stop [args] (stop))
```

`procrun` requires that the start and stop methods be static void and take an array of strings as their argument. To achieve this add

```
(:gen-class
    :methods [^:static [start ["[Ljava.lang.String;"] void]
              ^:static [stop ["[Ljava.lang.String;"] void]]
    :main false)
```

to your `ns` form.

### Install service

Run the following command

```
prunsrv //IS//SPARTADATA --DisplayName="SpartaData App" --Description="Web application and data repository for the Sparta Project." --Classpath C:\spartadata\target\spartadata.jar --StartMode jvm --StartClass spartadata.application --StartMethod start --StopMode  jvm --StopClass spartadata.application --StopMethod  stop --Startup=auto  --LogLevel=DEBUG --LogPath=C:\spartadata\log --LogPrefix=commons-daemon --StdOutput=C:\spartadata\log\stdout.log --StdError=C:\spartadata\log\stderr.log
```

To run the service as the `spartapp` user go to *Control Panel > System and Security > Administrative Tools > Services*, scroll to `Sparta App` and double click to open. Once open, go to the *Log On* tab and select *This account* and input `spartapp@resbank.co.za` and provide the account's password. Next, go to the *General* tab and start the application.

### Open port 80 to allow web traffic

To open port 80

1. From the Start menu, click Control Panel, click System and Security, and then click Windows Firewall. Control Panel is not configured for 'Category' view, you only need to select Windows Firewall.
2. Click Advanced Settings.
3. Click Inbound Rules.
4. Click New Rule in the Actions window.
5. Click Rule Type of Port.
6. Click Next.
7. On the Protocol and Ports page click TCP.
8. Select Specific Local Ports and type a value of 80.
9. Click Next.
10. On the Action page click Allow the connection.
11. Click Next.
12. On the Profile page click the appropriate options for your environment.
13. Click Next.
14. On the Name page enter a name of ReportServer (TCP on port 80)
15. Click Finish.
16. Restart the computer. 
