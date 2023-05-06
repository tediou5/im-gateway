echo "start server"

if [ -f "./im-infra-link" ]; then

    echo "start link server..."

    if [ -f "/tmp/link.pid" ]; then
    kill $(cat /tmp/link.pid)
    echo "kill link process $(cat /tmp/link.pid)"


    if [ -f "/tmp/link.log" ]; then
    rm -rf /tmp/link.log
    echo "link.log exist"
    fi

    nohup ./im-infra-link -c ./link-config >> /tmp/link.log 2>&1 &
    echo $! > /tmp/link.pid

fi

if [ -f "./im-infra-proxy" ]; then

    echo "start proxy server..."

    if [ -f "/tmp/proxy.pid" ]; then
    kill $(cat /tmp/proxy.pid)
    echo "kill proxy process $(cat /tmp/proxy.pid)"
    fi

    if [ -f "/tmp/proxy.log" ]; then
    rm -rf /tmp/proxy.log
    echo "proxy.log exist"
    fi

    nohup ./im-infra-proxy -c ./proxy-config >> /tmp/proxy.log 2>&1 &
    echo $! > /tmp/proxy.pid

fi

echo "#################################################"
echo "#        process    pid                          "
echo "#################################################"

if [ -f "/tmp/link.pid" ]; then
echo "#        link       $(cat /tmp/link.pid)"
fi

if [ -f "/tmp/proxy.pid" ]; then
echo "#        proxy      $(cat /tmp/proxy.pid)"
fi

echo "#################################################"
