// ==========================================================================
//  Squidex Headless CMS
// ==========================================================================
//  Copyright (c) Squidex UG (haftungsbeschraenkt)
//  All rights reserved. Licensed under the MIT license.
// ==========================================================================

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squidex.Caching;
using Squidex.Events;
using Squidex.Infrastructure.EventSourcing;
using Squidex.Infrastructure.States;
using Squidex.Infrastructure.Tasks;
using System.Runtime.CompilerServices;

namespace Squidex.Infrastructure.Commands;

public class Rebuilder(
    IDomainObjectFactory domainObjectFactory,
    ILocalCache localCache,
    IEventStore eventStore,
    IServiceProvider serviceProvider,
    ILogger<Rebuilder> log)
{
    public virtual async Task<T> RebuildStateAsync<T, TState>(DomainId id,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        var store = serviceProvider.GetRequiredService<IStore<TState>>();

        var domainObject = domainObjectFactory.Create<T, TState>(id, store);

        await domainObject.EnsureLoadedAsync(ct);

        return domainObject;
    }

    public virtual Task RebuildAsync<T, TState>(StreamFilter filter, int batchSize,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        return RebuildAsync<T, TState>(filter, batchSize, 0, ct);
    }

    public virtual async Task RebuildAsync<T, TState>(StreamFilter filter, int batchSize, double errorThreshold,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        await ClearAsync<TState>();

        var ids = SelectIds(eventStore.QueryAllAsync(filter, ct: ct), ct);

        await InsertManyAsync<T, TState>(ids, batchSize, errorThreshold, ct);
    }

    public virtual Task InsertManyAsync<T, TState>(IEnumerable<DomainId> source, int batchSize,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        return InsertManyAsync<T, TState>(source, batchSize, 0, ct);
    }

    public virtual async Task InsertManyAsync<T, TState>(IEnumerable<DomainId> source, int batchSize, double errorThreshold = 0,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        Guard.NotNull(source);

        var ids = ToAsyncEnumerable(source, ct);

        await InsertManyAsync<T, TState>(ids, batchSize, errorThreshold, ct);
    }

    private async Task InsertManyAsync<T, TState>(IAsyncEnumerable<DomainId> source, int batchSize, double errorThreshold,
        CancellationToken ct = default)
        where T : DomainObject<TState> where TState : Entity, new()
    {
        var store = serviceProvider.GetRequiredService<IStore<TState>>();

        var handledIds = new HashSet<DomainId>();
        var handlerErrors = 0;

        using (localCache.StartContext())
        {
            var batches = BatchIds(source, handledIds, batchSize, ct);

            await Parallel.ForEachAsync(batches, ct, async (batch, ct) =>
            {
                await using (var context = store.WithBatchContext(typeof(T)))
                {
                    await context.LoadAsync(batch);

                    foreach (var id in batch)
                    {
                        try
                        {
                            var domainObject = domainObjectFactory.Create<T, TState>(id, context);

                            await domainObject.RebuildStateAsync(ct);
                        }
                        catch (DomainObjectNotFoundException)
                        {
                            return;
                        }
                        catch (Exception ex)
                        {
                            log.LogWarning(ex, "Found corrupt domain object of type {type} with ID {id}.", typeof(T), id);
                            Interlocked.Increment(ref handlerErrors);
                        }
                    }
                }
            });
        }

        var errorRate = handledIds.Count == 0 ? 0 : (double)handlerErrors / handledIds.Count;

        if (errorRate > errorThreshold)
        {
            ThrowHelper.InvalidOperationException($"Error rate of {errorRate} is above threshold {errorThreshold}.");
        }
    }

    private static async IAsyncEnumerable<DomainId> SelectIds(IAsyncEnumerable<StoredEvent> source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
        {
            yield return item.Data.Headers.AggregateId();
        }
    }

    private static async IAsyncEnumerable<DomainId> ToAsyncEnumerable(IEnumerable<DomainId> source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var id in source)
        {
            ct.ThrowIfCancellationRequested();

            yield return id;
        }
    }

    private static async IAsyncEnumerable<IReadOnlyList<DomainId>> BatchIds(IAsyncEnumerable<DomainId> source, HashSet<DomainId> handledIds, int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var buffer = new List<DomainId>(batchSize);

        await foreach (var id in source.WithCancellation(ct))
        {
            if (!handledIds.Add(id))
            {
                continue;
            }

            buffer.Add(id);

            if (buffer.Count >= batchSize)
            {
                yield return buffer.ToArray();
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            yield return buffer.ToArray();
        }
    }

    private async Task ClearAsync<TState>() where TState : Entity, new()
    {
        var store = serviceProvider.GetRequiredService<IStore<TState>>();

        await store.ClearSnapshotsAsync();
    }
}
