using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Basic.Common;

namespace Basic.CarSvc.Controllers
{
	[Route("api/[controller]")]
	public class CarsController : Controller
	{
		private readonly IReliableStateManager _stateManager;

		public CarsController(IReliableStateManager stateManager)
		{
			_stateManager = stateManager;
		}

		// GET api/cars
		[HttpGet]
		public async Task<IActionResult> Get(CancellationToken token)
		{
			var cars = await _stateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");
			using (var tx = _stateManager.CreateTransaction())
			{
				var results = new List<Car>();

				var enumerable = await cars.CreateEnumerableAsync(tx);
				var enumerator = enumerable.GetAsyncEnumerator();
				while (await enumerator.MoveNextAsync(token))
				{
					results.Add(enumerator.Current.Value);
				}

				await tx.CommitAsync();

				return Ok(results);
			}
		}

		// GET api/cars/J1234
		[HttpGet("{vin}")]
		public async Task<IActionResult> Get(string vin)
		{
			var cars = await _stateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");
			using (var tx = _stateManager.CreateTransaction())
			{
				var result = await cars.TryGetValueAsync(tx, vin);
				await tx.CommitAsync();

				return result.HasValue ? Ok(result.Value) : (IActionResult)NotFound();
			}
		}

		// POST api/cars
		[HttpPost]
		public async Task<IActionResult> Post([FromBody]Car car)
		{
			var cars = await _stateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");
			using (var tx = _stateManager.CreateTransaction())
			{
				var result = await cars.TryAddAsync(tx, car.VIN, car);
				await tx.CommitAsync();

				return StatusCode((int)(result ? HttpStatusCode.OK : HttpStatusCode.Conflict));
			}
		}

		// PUT api/cars/J1234
		[HttpPut("{vin}")]
		public async Task<IActionResult> Put(string vin, [FromBody]Car car)
		{
			var cars = await _stateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");
			using (var tx = _stateManager.CreateTransaction())
			{
				await cars.SetAsync(tx, car.VIN, car);
				await tx.CommitAsync();

				return Ok();
			}
		}

		// DELETE api/cars/J1234
		[HttpDelete("{vin}")]
		public async Task<IActionResult> Delete(string vin)
		{
			var cars = await _stateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");
			using (var tx = _stateManager.CreateTransaction())
			{
				var result = await cars.TryRemoveAsync(tx, vin);
				await tx.CommitAsync();

				return result.HasValue ? Ok(result.Value) : (IActionResult)NotFound();
			}
		}
	}
}
