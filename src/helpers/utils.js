import os from 'os'

const getOSNetworkExternalInterface = () => {
  let ifaces = os.networkInterfaces()
  return Object.keys(ifaces).reduce((ips, ifaceName) => {
    return ips.concat(ifaces[ifaceName].filter(address => !address.internal && address.family === 'IPv4'))
  }, []).map(({address}) => address)[0] || undefined
}

export {
  getOSNetworkExternalInterface
}
