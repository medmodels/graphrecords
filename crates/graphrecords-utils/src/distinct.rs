use crate::aliases::GrHashSet;
use std::hash::Hash;

pub struct Distinct<T: Clone + Eq + Hash> {
    seen: GrHashSet<T>,
    elements: Vec<T>,
}

impl<T: Clone + Eq + Hash> Default for Distinct<T> {
    fn default() -> Self {
        Self {
            seen: GrHashSet::default(),
            elements: Vec::new(),
        }
    }
}

impl<T: Clone + Eq + Hash> FromIterator<T> for Distinct<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iterator: I) -> Self {
        let mut distinct = Self::default();
        distinct.extend(iterator);

        distinct
    }
}

impl<T: Clone + Eq + Hash> Extend<T> for Distinct<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iterator: I) {
        for element in iterator {
            if self.seen.insert(element.clone()) {
                self.elements.push(element);
            }
        }
    }
}

impl<T: Clone + Eq + Hash> IntoIterator for Distinct<T> {
    type IntoIter = std::vec::IntoIter<T>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

impl<T: Clone + Eq + Hash> From<Distinct<T>> for Vec<T> {
    fn from(distinct: Distinct<T>) -> Self {
        distinct.elements
    }
}

#[cfg(test)]
mod test {
    use super::Distinct;

    #[test]
    fn test_default() {
        let distinct: Distinct<&str> = Distinct::default();

        assert_eq!(Vec::<&str>::new(), Vec::from(distinct));
    }

    #[test]
    fn test_from_iter() {
        let distinct: Distinct<_> = ["lorem", "lorem", "ipsum", "dolor", "ipsum"]
            .into_iter()
            .collect();

        assert_eq!(
            vec!["lorem", "ipsum", "dolor"],
            distinct.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extend() {
        let mut distinct: Distinct<_> = ["lorem", "ipsum"].into_iter().collect();
        distinct.extend(["dolor", "lorem", "sed", "ipsum"]);

        assert_eq!(
            vec!["lorem", "ipsum", "dolor", "sed"],
            distinct.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_into_iter() {
        let distinct: Distinct<_> = ["sed", "lorem", "sed", "ipsum", "lorem", "dolor"]
            .into_iter()
            .collect();

        assert_eq!(
            vec!["sed", "lorem", "ipsum", "dolor"],
            distinct.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_from() {
        let distinct: Distinct<_> = ["lorem", "ipsum", "lorem", "dolor"].into_iter().collect();

        assert_eq!(vec!["lorem", "ipsum", "dolor"], Vec::from(distinct));
    }
}
