
const express = require('express');
const multer = require('multer');
const amqp = require('amqplib');
const ffmpeg = require('ffmpeg-static-electron');
const MongoClient = require('mongodb').MongoClient;

 const fs = require('fs');
 const path = require('path');

 const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });


(async () => {
  const conexao = await amqp.connect('amqp://localhost:3000');
  const canal = await conexao.createChannel();
  const concat = 'concat_queue';

  await canal.assertQueue(q, { durable: false });

  console.log('Aguardando mensagens na fila', concat);

  canal.consume(concat, async (msg) => {
    const { videoId, videoBuffer } = JSON.parse(msg.content.toString());

    try {
      const inputPath = path.join(__dirname, 'input', `${videoId}.mp4`);
      const outputPath = path.join(__dirname, 'output', `${videoId}.mp4`);

      fs.writeFileSync(inputPath, videoBuffer);

      await new Promise((resolve, reject) => {
        ffmpeg()
          .input(inputPath)
          .concat(inputPath)
          .output(outputPath)
          .on('end', resolve)
          .on('error', reject)
          .run();
      });

      await client.connect();
      const db = client.db('video_db');
      const collection = db.collection('videos');

      await collection.updateOne({ _id: videoId }, { $set: { status: 'completed', url: `http://localhost:3000/output/${videoId}.mp4` } });
     
    } catch (erro) {
      console.error(erro);

      await client.connect();
      const db = client.db('video_db');
      const collection = db.collection('videos');

      await collection.updateOne({ _id: videoId }, { $set: { status: 'erro' } });
    }
  });
})();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const port = process.env.PORT || 3000;

const inputPath = path.join(__dirname, 'input');
const outputPath = path.join(__dirname, 'output');

if (!fs.existsSync(inputPath)) {
  fs.mkdirSync(inputPath);
}

if (!fs.existsSync(outputPath)) {
  fs.mkdirSync(outputPath);
}

app.post('/concat', upload.single('file'), async (req, res) => {
  const concexao = await amqp.connect('amqp://localhost:3000');
  const canal = await conexao.createChannel();
  const concat = 'concat_queue';

  await canal.assertQueue(q, { durable: false });

  const videoBuffer = req.file.buffer;
  const videoId = Date.now();

  await client.connect();
  const db = client.db('video_db');
  const collection = db.collection('videos');

  await collection.insertOne({ _id: videoId, status: 'em processamento' });

  const msg = { videoId, videoBuffer };
  canal.sendToQueue(q, Buffer.from(JSON.stringify(msg)));

  res.status(200).json({ message: 'O vídeo está sendo processado', videoId });
});

app.get('/concat/:videoId', async (req, res) => {
  const videoId = req.params.videoId;

  await client.connect();
  const db = client.db('video_db');
  const collection = db.collection('videos');

  const video = await collection.findOne({ _id: parseInt(videoId) });

  if (!video) {
    return res.status(404).json({ message: 'Vídeo não encontrado' });
  }
})