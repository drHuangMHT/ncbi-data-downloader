#[macro_export]
macro_rules! with_retry {
    ($retry:expr=>$($tt:tt)+) => {
        {
            let mut retry_times = $retry;
            loop {
                match $($tt)+.await{
                    Ok(v) => break Ok(v),
                    Err(_) => {
                        retry_times += 1;
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
                }
                if retry_times >= $retry {
                    break Err(());
                }
            }
        }
    };
}

#[macro_export]
macro_rules! join_task {
    ($ident:ident in $iterable:ident=>{
        $($task:tt)+
    }
    ) => {
        {   
            use tokio::task::JoinSet;
            let mut join_set = JoinSet::new();
            for $ident in $iterable.clone(){
                let task = {$($task)+};
                join_set.spawn(task);
            }
            let total = $iterable.len();
            let mut success = 0;
            while let Some(result) = join_set.join_next().await{
                if let Err(e)=result{
                    println!("Internal Error: Cannot read join result: {:?}",e);
                    continue;
                }
                if let Ok(Ok(ok_result)) = result{
                    success += 1;
                    $iterable.retain(|v|*v != ok_result.0);
                }
            }
            println!("{} out of {} succeeded, failed tasks: {:?}",success,total,$iterable)
        }
    };
}
