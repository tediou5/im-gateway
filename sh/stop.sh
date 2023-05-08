echo "stop server"

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

if [ -f "/tmp/link.pid" ]; then

echo "stop link server..."
kill $(cat /tmp/link.pid)
echo "kill link process $(cat /tmp/link.pid)"
fi

if [ -f "/tmp/proxy.pid" ]; then

echo "stop proxy server..."
kill $(cat /tmp/proxy.pid)
echo "kill proxy process $(cat /tmp/proxy.pid)"
fi
