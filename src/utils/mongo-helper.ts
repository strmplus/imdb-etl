import { MongoClient } from 'mongodb';

export class MongoHelper {
  private readonly client: MongoClient;

  constructor() {
    this.client = new MongoClient(process.env.MONGO_URL);
  }

  async getCollection(dbName: string, collectionName: string) {
    try {
      await this.client.connect();
      return this.client.db(dbName).collection(collectionName);
    } catch (error) {
      await this.client.close();
      throw error;
    }
  }
}
