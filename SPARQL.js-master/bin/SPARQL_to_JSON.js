var SPARQL = '';
var SparqlParser = require('../sparql').Parser;

var fs= require('fs') 
var SPARQL_buffer = fs.createReadStream('SPARQL_buffer.sparql',  {encoding:'utf8'}); /////JS的文件读取流
//SPARQL_buffer.setEncoding('utf8');
//////当有数据可读时触发，并执行function
SPARQL_buffer.on('data', function (chunk) { SPARQL += chunk; });
//////当没有更多的数据可读时触发，并执行function
SPARQL_buffer.on('end', function () {
    try {
        var SPARQL_to_JSON = new SparqlParser().parse(SPARQL);
    } catch(error) {
        process.stdout.write(error.message + '\n');
        ////// 如果不可解析，则会抛出异常(throw error;)。JSON内容设置为null
        ////// 常见的不可解析的原因有：（1）prefix不全，例如rdf,dbp （2）工具不健全，生僻语法不可解析
        var SPARQL_to_JSON = null;
    } finally {
        SPARQL_to_JSON = JSON.stringify(SPARQL_to_JSON, null, 4);
        var JSON_buffer = fs.createWriteStream('JSON_buffer.json', {encoding:'utf8'}); /////JS的文件写入流
        JSON_buffer.write(SPARQL_to_JSON);
        JSON_buffer.close();
        var mutex = fs.createWriteStream('mutex.txt', {encoding:'utf8'}); /////JS的文件写入流
        mutex.write('1');
        mutex.close();
    }
});
////// 因为 SPARQL_buffer.on('data'); 和 SPARQL_buffer.on('end'); 是并发的监控触发机制，所以不能执行 SPARQL_buffer.close(); 