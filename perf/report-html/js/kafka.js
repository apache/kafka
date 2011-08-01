function kafkagraph(container, desc, fileName, ytitle) 
{
	var options = {
		chart: {
			renderTo: container,
			defaultSeriesType: 'line'
		},
		title: {
			text: desc
		},
		xAxis: {
			categories: [],
			title: {
			    text : ''
			}
		},
		yAxis: {
     		title: {
						text: ytitle
					}
		},
		series: []
	};
	$.get(fileName, function(data) {
		// Split the lines
		var servers = [];
		var serverCnt = 0;
		var MultiArray = new Array();
		var times = [];
		var lines = data.split('\n');
		var xtitle;
		$.each(lines, function(lineNo, line) {
			var items = line.split(',');
			
			// header line containes categories
			if (lineNo == 0) {
			    var xtitleRead = 0;
				$.each(items, function(itemNo, item) {
				    if(xtitleRead == 0)
					{
					   xtitle = item;
					   xtitleRead = 1;
					 }
					 else{
     					servers.push(item)
	    				serverCnt++
						}
				});
				for (i=0;i<serverCnt;i++)
				{
				   MultiArray[i] = [];
				}
			}
			
			// the rest of the lines contain data with their name in the first position
			else {
				
				

				$.each(items, function(itemNo, item) {
					if (itemNo == 0) {
						times.push(item)
					} else {
						MultiArray[itemNo-1].push(parseFloat(item));
					}
				});
			}
		});
		options.xAxis.title.text = xtitle;
		//options.series.push(series);
		for (i=0;i<serverCnt;i++)
		{
			var series = {
			 data: []
			};
			series.name = servers[i];
			for (j=0;j<MultiArray[i].length;j++)
			{
			  series.data.push(MultiArray[i][j])
			}
			options.series.push(series);
		}

		for (i=0;i<times.length;i++)
		{
		   options.xAxis.categories.push(times[i]);
		}					
	
		var chart = new Highcharts.Chart(options);
	});
}
