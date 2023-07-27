use crate::error::Result;

use super::Node;



/// 优化器
pub trait Optimizer {
    fn optimize(&self, node: Node) -> Result<Node>;
}