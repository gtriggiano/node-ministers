import Client from './Client'
import Minister from './Minister'
import Worker from './Worker'
import MINISTERS from './MINISTERS'

const lib = {
  Client,
  Minister,
  Worker,
  ...MINISTERS
}

export default lib
