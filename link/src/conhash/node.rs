pub trait Node: Clone {
    fn name(&self) -> String;
}

impl<T: ToString + Clone> Node for T {
    fn name(&self) -> String {
        self.to_string()
    }
}
