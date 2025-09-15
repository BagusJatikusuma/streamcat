package id.stream.streamcat.stream

case class Event(id: String, cmd: Command)

enum Command:
  case InitWorker(name: String)
  case MakeWork(url: String)
  case FireWorker(name: String)
