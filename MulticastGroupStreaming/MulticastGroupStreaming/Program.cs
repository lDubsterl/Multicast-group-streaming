using MulticastGroupStreaming.Services;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Mvc;
using System.Net;

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Add services to the container.
builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var dir = config["Directory"];
var address = config["MulticastGroupAddress"];
var port = config["Port"];
if (dir != null && address != null && port != null)
    builder.Services.AddSingleton(new SendService(dir, address, int.Parse(port)));

var app = builder.Build();

//Configure the HTTP request pipeline.
//if (app.Environment.IsDevelopment())
//{
app.UseSwagger();
app.UseSwaggerUI();
//}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
