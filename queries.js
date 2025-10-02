// queries.js - MongoDB queries for PLP Bookstore Assignment

// Import MongoDB client
const { MongoClient } = require('mongodb');

// Connection URI
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

// Connect to MongoDB
async function connectToMongoDB() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);
  const collection = db.collection(collectionName);
  return { client, collection };
}

// Task 2: Basic CRUD Operations
async function basicCRUD() {
  const { client, collection } = await connectToMongoDB();
  
  try {
    // 1. Find all books in a specific genre
    console.log('\n1. Books in Fiction genre:');
    const fictionBooks = await collection.find({ genre: 'Fiction' }).toArray();
    console.log(fictionBooks);

    // 2. Find books published after a certain year (e.g., 1950)
    console.log('\n2. Books published after 1950:');
    const recentBooks = await collection.find({ published_year: { $gt: 1950 } }).toArray();
    console.log(recentBooks);

    // 3. Find books by a specific author
    console.log('\n3. Books by George Orwell:');
    const orwellBooks = await collection.find({ author: 'George Orwell' }).toArray();
    console.log(orwellBooks);

    // 4. Update the price of a specific book
    console.log('\n4. Updating price of "1984" to 11.99');
    const updateResult = await collection.updateOne(
      { title: '1984' },
      { $set: { price: 11.99 } }
    );
    console.log(`Updated ${updateResult.modifiedCount} document`);

    // 5. Delete a book by its title
    console.log('\n5. Deleting "Moby Dick"');
    const deleteResult = await collection.deleteOne({ title: 'Moby Dick' });
    console.log(`Deleted ${deleteResult.deletedCount} document`);

  } finally {
    await client.close();
  }
}

// Task 3: Advanced Queries
async function advancedQueries() {
  const { client, collection } = await connectToMongoDB();
  
  try {
    // 1. Find books that are both in stock and published after 2010
    console.log('\n1. In-stock books published after 2010:');
    const inStockRecent = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log(inStockRecent);

    // 2. Use projection to return only specific fields
    console.log('\n2. Projection (title, author, price):');
    const projectedBooks = await collection.find({}, {
      projection: { title: 1, author: 1, price: 1, _id: 0 }
    }).toArray();
    console.log(projectedBooks);

    // 3. Sort books by price (ascending and descending)
    console.log('\n3. Books sorted by price (ascending):');
    const sortedAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log(sortedAsc.map(b => `${b.title}: $${b.price}`));

    console.log('\n   Books sorted by price (descending):');
    const sortedDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log(sortedDesc.map(b => `${b.title}: $${b.price}`));

    // 4. Pagination (5 books per page, 2nd page)
    console.log('\n4. Pagination (page 2, 5 books per page):');
    const page = 2;
    const perPage = 5;
    const paginatedBooks = await collection.find()
      .skip((page - 1) * perPage)
      .limit(perPage)
      .toArray();
    console.log(paginatedBooks);

  } finally {
    await client.close();
  }
}

// Task 4: Aggregation Pipeline
async function aggregationPipelines() {
  const { client, collection } = await connectToMongoDB();
  
  try {
    // 1. Calculate average price by genre
    console.log('\n1. Average price by genre:');
    const avgPriceByGenre = await collection.aggregate([
      {
        $group: {
          _id: '$genre',
          avgPrice: { $avg: '$price' },
          count: { $sum: 1 }
        }
      },
      { $sort: { avgPrice: -1 } }
    ]).toArray();
    console.log(avgPriceByGenre);

    // 2. Find author with most books
    console.log('\n2. Author with most books:');
    const prolificAuthor = await collection.aggregate([
      {
        $group: {
          _id: '$author',
          bookCount: { $sum: 1 }
        }
      },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log(prolificAuthor);

    // 3. Group books by publication decade and count them
    console.log('\n3. Books by publication decade:');
    const booksByDecade = await collection.aggregate([
      {
        $project: {
          decade: {
            $multiply: [
              { $floor: { $divide: ['$published_year', 10] } },
              10
            ]
          }
        }
      },
      {
        $group: {
          _id: '$decade',
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log(booksByDecade);

  } finally {
    await client.close();
  }
}

// Task 5: Indexing
async function manageIndexes() {
  const { client, collection } = await connectToMongoDB();
  
  try {
    // 1. Create index on title field
    console.log('\n1. Creating index on title field...');
    await collection.createIndex({ title: 1 });
    console.log('Index on title created');

    // 2. Create compound index on author and published_year
    console.log('\n2. Creating compound index on author and published_year...');
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log('Compound index created');

    // 3. Demonstrate performance improvement with explain()
    console.log('\n3. Performance comparison with explain():');
    
    console.log('\nWithout index (on genre):');
    const withoutIndex = await collection.find({ genre: 'Fiction' })
      .explain('executionStats');
    console.log(`Documents examined: ${withoutIndex.executionStats.totalDocsExamined}`);
    console.log(`Execution time (ms): ${withoutIndex.executionStats.executionTimeMillis}`);

    console.log('\nWith index (on title):');
    const withIndex = await collection.find({ title: '1984' })
      .explain('executionStats');
    console.log(`Documents examined: ${withIndex.executionStats.totalDocsExamined}`);
    console.log(`Execution time (ms): ${withIndex.executionStats.executionTimeMillis}`);

  } finally {
    await client.close();
  }
}

// Execute all tasks
async function main() {
  console.log('=== Basic CRUD Operations ===');
  await basicCRUD();
  
  console.log('\n=== Advanced Queries ===');
  await advancedQueries();
  
  console.log('\n=== Aggregation Pipelines ===');
  await aggregationPipelines();
  
  console.log('\n=== Indexing ===');
  await manageIndexes();
}

// Run the main function
main().catch(console.error);
