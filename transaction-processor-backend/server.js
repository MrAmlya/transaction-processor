const express = require('express');
const multer = require('multer');
const csvParser = require('csv-parser');
const redis = require('redis');
const fs = require('fs');
const path = require('path');

// Initialize the app and set up the port
const app = express();
const port = 3001;

// Enable CORS
const cors = require('cors');
app.use(cors());


// Set up Redis client for in-memory cache
const client = redis.createClient({
    host: '127.0.0.1',
    port: 6379
  });

// Connect to Redis and handle any connection errors
client.connect().catch(console.error);

// Use multer for handling file uploads, storing files in the 'uploads/' directory
const upload = multer({ dest: 'uploads/' });

// Middleware for parsing JSON data
app.use(express.json());

/**
 * Function to process the uploaded CSV file
 * @param {String} filePath - Path to the uploaded file
 * @returns {Promise} - Resolves with an array of transaction objects
 */
const processTransactions = (filePath) => {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', (data) => {
                // Push each parsed transaction data into the results array
                results.push(data);
            })
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};

// Route to handle file upload and transaction processing
app.post('/upload', upload.single('file'), async (req, res) => {
    try {
        const transactions = await processTransactions(req.file.path);
        // Store transactions in Redis as a JSON string
        await client.set('transactions', JSON.stringify(transactions));
        res.status(200).json({ message: 'Transactions processed successfully' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to process transactions' });
    }
});

// Route to generate account report: shows accounts, their cards, and balances
app.get('/report/accounts', async (req, res) => {
    const transactions = JSON.parse(await client.get('transactions')) || [];
    const accounts = {};

    // Process transactions and calculate balance for each card under each account
    transactions.forEach(tx => {
        const { 'Account Name': accountName, 'Transaction Amount': amount, 'Card Number': cardNumber } = tx;
        if (!accounts[accountName]) {
            accounts[accountName] = {};
        }
        if (!accounts[accountName][cardNumber]) {
            accounts[accountName][cardNumber] = 0;
        }
        accounts[accountName][cardNumber] += parseFloat(amount);
    });

    res.status(200).json(accounts);
});

// Route to list "bad transactions" - transactions that failed to parse or have invalid data
app.get('/report/bad-transactions', async (req, res) => {
    const transactions = JSON.parse(await client.get('transactions')) || [];
    const badTransactions = transactions.filter(tx => !tx['Account Name'] || !tx['Transaction Amount']);
    res.status(200).json(badTransactions);
});

// Route to generate collections report: accounts with cards having a negative balance
app.get('/report/collections', async (req, res) => {
    const transactions = JSON.parse(await client.get('transactions')) || [];
    const collections = [];

    // Identify accounts with negative balances on any card
    transactions.forEach(tx => {
        if (parseFloat(tx['Transaction Amount']) < 0) {
            collections.push(tx['Account Name']);
        }
    });

    res.status(200).json([...new Set(collections)]);
});

// Route to reset the system, clearing transactions from Redis
app.post('/reset', async (req, res) => {
    await client.del('transactions');
    res.status(200).json({ message: 'System reset successfully' });
});

// Start the server and listen on the defined port
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
