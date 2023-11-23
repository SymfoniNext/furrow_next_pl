import { Client, BeanstalkJobState } from 'node-beanstalk';
import pkg from 'protobufjs';

const { load } = pkg;

const c = new Client({
  host: '127.0.0.1',
  port: 11300,
});

var jobsTube = "jobs";

load('job.proto', (err, root) => {
  if (err)
    throw err;

  var job = root.lookupType("janochapack.Job");
  var payload = {
    RequestID: "12345",
    Image: "docker.io/library/redis:latest",
    Cmd: ["-m conversion -in /in/DOCUMENT_CONTENT-1146645.pdf -f PNG -image_width 200 -image_only_first_page"]
  }

  var errMsg = job.verify(payload);
  if (errMsg)
      throw Error(errMsg);

  var msg = job.create(payload);
  var buffer = job.encode(msg).finish();

  // console.log(buffer.buffer)

  c.connect().then(async () => {
    console.log('connected', c.isConnected)
    await c.use(jobsTube);

    c.put(payload).then(async (state) => {
      console.log('Message sent.', state)

      c.disconnect().then(() => console.log('Disconnected from beanstakd'))
    })
  }).catch(err => {
    console.error(err)
    console.error("\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
    console.error("Run port forwarding from kuberenets. Take a look at readme.md\n")
    console.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
  })

})

if(c.isConnected){
  c.disconnect()
}