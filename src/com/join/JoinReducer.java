package com.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Item, Item, NullWritable> {
	Map<String, Item> productMap=new HashMap<>();
	@Override
	protected void reduce(Text key, Iterable<Item> values, Reducer<Text, Item, Item, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		List<Item> itemList = new ArrayList<Item>();
		
		for(Item value:values){
			//--此处注意，因为迭代器用到了地址复用技术，不能直接存储 
			Item item=value.clone();
			//--在迭代器里，注意地址复用的知识点
			itemList.add(item);
			if(value.getId().equals("")){
				productMap.put(item.getPid(),item);
			}
		}
		
		for(Item value:itemList){
			if(!value.getId().equals("")){
			Item productItem=new Item();
			//--设置订单id
			productItem.setId(value.getId());
			//--设置订单日期
			productItem.setDate(value.getDate());
			//--设置出货量
			productItem.setAmount(value.getAmount());
			//--设置商品id
			productItem.setPid(value.getPid());
			//--设置商品标题
			productItem.setName(productMap.get(value.getPid()).getName());
			//--设置商品价格
			productItem.setPrice(productMap.get(value.getPid()).getPrice());
			
			context.write(productItem, NullWritable.get());
			}
		
		}
		
	}

}
