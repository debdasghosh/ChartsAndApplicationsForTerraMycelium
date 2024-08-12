import express from 'express';
import session from 'express-session';
import bodyParser from 'body-parser';
import sqlitePackage from 'sqlite3';
const { verbose } = sqlitePackage;
const sqlite3 = verbose();
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import Keycloak from 'keycloak-connect';
import vault from 'node-vault';

const app = express();

// setting this because unlike common JS modules, ES module does not include __dirname by default in all modules
const __dirname = path.dirname(fileURLToPath(import.meta.url));

app.set('views', path.join(__dirname, 'views'));

// setting Vault settings
const vaultOptions = {
    apiVersion: 'v1', // Vault API version
    endpoint: 'http://localhost:8200', // Vault endpoint
    token: 'root'
  };
  
  const vaultClient = vault(vaultOptions);

// setting the session for keycloak
const memoryStore = new session.MemoryStore();

// setting the session
app.use(session({
    secret: 'someSecret',
    resave: false,
    saveUninitialized: true
  }));
  
//   realm: "master",
//   serverUrl: "http://localhost/keycloak/",
//   sslRequired: "external",
//   resource: "data-lichen-client",
//   credentials: {
//     "secret": "XwFsfqyy4BAI1mH7XN4JJ6EuI9brfz2h"
//   },
//   "confidential-port": 0

async function fetchSecretsFromVault() {
try {
    const result = await vaultClient.read('secret/data/data-lichen-secret');
    return result.data.data; // This will contain your secrets
} catch (error) {
    console.error('Failed to fetch secrets from Vault:', error);
    process.exit(1); // Exit the application if you can't fetch necessary secrets
}
}

// Initialising Keycloak
const keycloakConfig = await fetchSecretsFromVault()

const keycloak = new Keycloak({store: memoryStore}, keycloakConfig);

// Middleware to protect routes
app.use(keycloak.middleware());

const KAFKA_PROXY_URL = 'http://localhost/kafka-rest-proxy';
const KAFKA_TOPIC = 'data-discovery';

app.use(bodyParser.json());
app.use(express.static('public'));

// Define your template engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));



// Initialize SQLite database
let db = new sqlite3.Database('./metadata.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the metadata database.');
});

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS metadata(
            uniqueIdentifier TEXT PRIMARY KEY,
            serviceName TEXT,
            serviceAddress TEXT,
            completeness REAL,
            validity INTEGER,
            accuracy REAL,
            processingTime TEXT,
            actualTime TEXT,
            processingDuration TEXT
            )`);
});

app.post('/register', async (req, res) => {
    const data = req.body;
    const updateSql = `REPLACE INTO metadata(uniqueIdentifier, serviceName, serviceAddress, completeness, validity, accuracy, processingTime, actualTime, processingDuration)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`; // Added the processingDuration

    db.run(updateSql,
        [data.uniqueIdentifier, data.serviceName, data.serviceAddress, data.completeness, data.validity, data.accuracy, data.processingTime, data.actualTime, data.processingDuration],  // Added the processingDuration
        function(err) {
            if (err) {
                return console.log(err.message);
            }
            console.log(`A row has been inserted/updated with UUID ${data.uniqueIdentifier}`);
        });

    res.json({ message: 'Metadata registered successfully.' });
});

app.get("/", (req,res) => {
    res.json("Welcome to Datalichen")
})

app.get('/metadata', async (req, res) => {
    let sql = `SELECT DISTINCT * FROM metadata`;  // Ensure unique services
    db.all(sql, [], (err, rows) => {
        if (err) {
            throw err;
        }
        console.log(rows)
        res.render('metadata', { metadata: rows });
    });
});

app.listen(3001, () => {
    console.log('Server is running on port 3001');
});


app.get('/publish-metadata', async (req, res) => {
    try {
        // 1. Fetch metadata from SQLite
        let sql = `SELECT DISTINCT * FROM metadata`;  // Ensure unique services
        let metadata = await new Promise((resolve, reject) => {
            db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });

        // 2. Produce metadata to the Kafka topic via Kafka REST Proxy
        const produceData = metadata.map(item => ({
            value: item
        }));

        await axios.post(`${KAFKA_PROXY_URL}/topics/${KAFKA_TOPIC}`, {
            records: produceData
        }, {
            headers: {
                'Content-Type': 'application/vnd.kafka.json.v2+json'
            }
        });

        res.json({ message: 'Metadata fetched and sent to Kafka successfully' });

    } catch (error) {
        console.error('Error fetching from database or sending to Kafka:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/some-protected-route', keycloak.protect(), (req, res) => {
    res.send('This is protected!'); 
  });

// This is a test endpoint, uncomment if needs be

// app.get('/get-secrets', async (req,res) => {
//     async function fetchSecretsFromVault() {
//         try {
//           const result = await vaultClient.read('secret/data/data-lichen-secret');
//           return result.data.data; // This will contain your secrets
//         } catch (error) {
//           console.error('Failed to fetch secrets from Vault:', error);
//           process.exit(1); // Exit the application if you can't fetch necessary secrets
//         }
//       }
    
//       const secrets = await fetchSecretsFromVault()
//       console.log(secrets);

// })