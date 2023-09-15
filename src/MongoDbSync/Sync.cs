using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoDbSync;

internal class Sync
{
    readonly IMongoClient _sourrce;
    readonly IMongoClient _target;
    readonly DateTimeOffset _startPoint;
    readonly IEnumerable<string> _dbs;


    public Sync(string sourceConnString, string targetConnString, IEnumerable<string> dbs, int noOfHours)
    {
        var sourceClientSettings = MongoClientSettings.FromConnectionString(sourceConnString);
        var targetClientSettings = MongoClientSettings.FromConnectionString(targetConnString);

        _sourrce = new MongoClient(sourceClientSettings);
        _target = new MongoClient(targetClientSettings);

        _dbs = dbs;
        _startPoint = DateTimeOffset.UtcNow.AddHours(noOfHours * -1);
    }

    public async Task Execute()
    {
        var options = new ChangeStreamOptions
        {
            BatchSize = 2000,
            StartAtOperationTime = new BsonTimestamp((int)_startPoint.ToUnixTimeSeconds(), 1),
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,

        };

        using var cursor = await _sourrce.WatchAsync(options);
        long count = 0;
        while (await cursor.MoveNextAsync())
        {
            if (!cursor.Current.Any())
            {
                Console.WriteLine("No changes, skip...");
                continue;
            }

            var token = cursor.GetResumeToken();
            var headDoc = cursor.Current.First();
            Console.WriteLine($"{DelayedSeconds(headDoc.ClusterTime.Timestamp)}s, {count}, Token={token} ----------------------------");

            foreach (var change in cursor.Current)
            {
                ++count;
                await UpdateTarget(change);
            }
        }
    }

    private static long DelayedSeconds(int clusterTimestamp) => DateTimeOffset.UtcNow.ToUnixTimeSeconds() - clusterTimestamp;


    private async Task UpdateTarget(ChangeStreamDocument<BsonDocument> change)
    {
        var dbName = change.CollectionNamespace.DatabaseNamespace.ToString();
        if (!_dbs.Contains(dbName,StringComparer.OrdinalIgnoreCase))
            return;

        var collectionName = change.CollectionNamespace.CollectionName.ToString();
        var collection = _target.GetDatabase(dbName).GetCollection<BsonDocument>(collectionName);

        // Uncomment the following two lines to print changes without replaying to the target
        //Console.WriteLine($"{DelayedSeconds(change.ClusterTime.Timestamp)}s {change.CollectionNamespace.FullName} {change.OperationType}");
        //return;

        try
        {
            using var session = await _target.StartSessionAsync();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await session.WithTransactionAsync(
                async (s, ct) =>
                {
                    if (change.OperationType == ChangeStreamOperationType.Insert)
                    {
                        await collection.InsertOneAsync(s, change.FullDocument, cancellationToken: ct);
                    }
                    else if (change.OperationType == ChangeStreamOperationType.Delete)
                    {
                        var id = change.DocumentKey.GetValue("_id").ToString();
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                        await collection.DeleteOneAsync(s, filter, cancellationToken: ct);
                    }
                    else if (change.OperationType == ChangeStreamOperationType.Update || change.OperationType == ChangeStreamOperationType.Replace)
                    {
                        var id = change.FullDocument.GetValue("_id").ToString();
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                        await collection.ReplaceOneAsync(s, filter, change.FullDocument, cancellationToken: ct);
                    }
                    else
                    {
                        Console.WriteLine($"Unknown type={change.OperationType}");
                    }

                    return string.Empty;
                }, cancellationToken: cts.Token);
        }
        catch (MongoWriteException ex)
        {
            if (ex.WriteError.Code == 11000)
            {
                Console.WriteLine($"{DelayedSeconds(change.ClusterTime.Timestamp)}s, DupKey={change.FullDocument.GetValue("_id")}, ignore...");
            }
            else
            {
                Console.WriteLine($"{DelayedSeconds(change.ClusterTime.Timestamp)}s, #{change.OperationType} MongoWriteException={change.ToJson()}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{DelayedSeconds(change.ClusterTime.Timestamp)}s, #{change.OperationType} Exception={change.DocumentKey.ToJson()}, Type={ex.GetType()}, Message={ex.Message}");
        }
    }

}
